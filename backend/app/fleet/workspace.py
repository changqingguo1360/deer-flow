"""Private workspace adapters bound by the original trusted runner host."""

import asyncio
from dataclasses import asdict, fields

from deerflow_ecs_fleet.persistence.models import AttemptRow, WorkspaceProcessRow
from sqlalchemy import func, select

from deerflow.runtime.execution.mutation_context import OwnershipRejected, current_remote_mutation_context
from deerflow.runtime.execution.workspace_process import OriginalToolProcess, supervisor_source_digest
from deerflow.runtime.execution.workspace_supervisor import process_identity

from .mutation import _SessionCursor


async def _exact_root(session, identity, *, ancestor=None):
    from sqlalchemy import text

    rows = (
        (await session.execute(text("SELECT checkpoint_id,parent_checkpoint_id,metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1"), {"thread": identity.thread_id})).mappings().first()
    )
    if rows is None or rows["checkpoint_id"] != identity.checkpoint_id or rows["metadata"].get("deerflow_execution_run_id") != identity.run_id:
        raise OwnershipRejected("Workspace candidate differs from original current root/run")
    if ancestor is None:
        return
    seen = set()
    current = rows
    for _ in range(10000):
        if current["checkpoint_id"] == ancestor:
            return
        parent = current["parent_checkpoint_id"]
        if not parent or parent in seen:
            break
        seen.add(parent)
        current = (
            (
                await session.execute(
                    text("SELECT checkpoint_id,parent_checkpoint_id,metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"), {"thread": identity.thread_id, "checkpoint": parent}
                )
            )
            .mappings()
            .first()
        )
        if current is None or current["metadata"].get("deerflow_execution_run_id") != identity.run_id:
            break
    raise OwnershipRejected("Original presentation checkpoint ancestry rejected")


class FleetWorkspaceProcessRegistry:
    """Fence actual original Popen identities on the original SQL writer TX."""

    def __init__(self, session_factory, capability, *, pids_limit, execution_deadline=None):
        self.sf = session_factory
        self.capability = capability
        self.pids_limit = pids_limit
        self.execution_deadline = execution_deadline

    def _validate(self, session, *, allow_terminal=False, deadline=None):
        import time

        from sqlalchemy import text

        deadline = self.execution_deadline if deadline is None else deadline
        if deadline is None or deadline <= time.monotonic():
            raise OwnershipRejected("Original process SQL deadline elapsed")
        milliseconds = str(max(1, int(min(10, deadline - time.monotonic()) * 1000)))
        session.execute(text("SELECT set_config('lock_timeout', :limit, true), set_config('statement_timeout', :limit, true)"), {"limit": milliseconds})
        if current_remote_mutation_context() != self.capability.context:
            raise OwnershipRejected("Original workspace process scope required")
        if session.get_bind().dialect.name != "postgresql":
            raise OwnershipRejected("Workspace process registry requires PostgreSQL")
        asyncio.run(self.capability._guard.validate(_SessionCursor(session, synchronous=True), thread_id=self.capability.context.thread_id, operation="workspace.process", allow_terminal=allow_terminal))
        attempt = session.get(AttemptRow, self.capability.context.attempt_id)
        if ((attempt.launch_spec or {}).get("execution_profile") or {}).get("pids_limit") != self.pids_limit:
            raise OwnershipRejected("Workspace process limit differs from original frozen profile")

    def register(self, record, actual_popen):
        if not isinstance(record, OriginalToolProcess) or record.source_digest != supervisor_source_digest():
            raise OwnershipRejected("Installed workspace supervisor identity required")
        fresh = process_identity(record.pid)
        if fresh["start_ticks"] != record.start_ticks:
            raise OwnershipRejected("Workspace process PID identity changed")
        expected_parent = record.supervisor_pid if record.role == "shell" else actual_popen.pid
        if record.role == "supervisor":
            if record.pid != actual_popen.pid or actual_popen.poll() is not None:
                raise OwnershipRejected("Original supervisor Popen handle mismatch")
        elif record.role == "shell":
            if expected_parent != actual_popen.pid or fresh["ppid"] != actual_popen.pid:
                raise OwnershipRejected("Original shell supervisor Popen handle mismatch")
        else:
            raise OwnershipRejected("Unknown workspace process role")
        context = self.capability.context
        owner = {field.name: getattr(context, field.name) for field in fields(context)}
        owner["process_ref"] = "fleet-" + context.attempt_id
        with self.sf.begin() as session:
            self._validate(session)
            existing = session.get(WorkspaceProcessRow, (context.attempt_id, record.pid, record.start_ticks))
            values = {**owner, **asdict(record)}
            if existing is not None:
                if any(getattr(existing, key) != value for key, value in values.items()):
                    raise OwnershipRejected("Original process registration identity conflicts")
            else:
                live = session.scalar(select(func.count()).select_from(WorkspaceProcessRow).where(WorkspaceProcessRow.attempt_id == context.attempt_id, WorkspaceProcessRow.state == "registered"))
                if live >= self.pids_limit:
                    raise OwnershipRejected("Frozen workspace process registration limit exceeded")
                session.add(WorkspaceProcessRow(**values))
                session.flush()
            # Physical start identity is rechecked after all SQL/unique waits.
            fresh = process_identity(record.pid)
            if fresh["start_ticks"] != record.start_ticks or actual_popen.poll() is not None or (record.role == "shell" and fresh["ppid"] != actual_popen.pid):
                raise OwnershipRejected("Original workspace Popen identity changed during registration")
            # Clock/lease are retaken after every SQL/unique wait, before commit.
            self._validate(session)

    def settled(self, record, *, deadline=None):
        try:
            fresh = process_identity(record.pid)
        except FileNotFoundError:
            fresh = None
        if fresh is not None and fresh["start_ticks"] == record.start_ticks:
            raise OwnershipRejected("Original workspace process has not physically settled")
        context = self.capability.context
        with self.sf.begin() as session:
            self._validate(session, allow_terminal=True, deadline=deadline)
            row = session.get(WorkspaceProcessRow, (context.attempt_id, record.pid, record.start_ticks), with_for_update=True)
            if row is None or any(getattr(row, key) != value for key, value in asdict(record).items()):
                raise OwnershipRejected("Original workspace process settlement identity conflicts")
            if row.state == "registered":
                row.state = "settled"
                row.settled_at = session.scalar(select(func.clock_timestamp()))
                session.flush()
            self._validate(session, allow_terminal=True, deadline=deadline)


class FleetWorkspacePublisher:
    """Private original-host barrier and durable publication, never point acceptance."""

    def __init__(self, session_factory, capability, *, controller, teardown, session_pool):
        if teardown.context != capability.context or teardown.workspace_writers is not controller:
            raise OwnershipRejected("Original workspace teardown/controller pair required")
        self.sf, self.capability = session_factory, capability
        self.controller, self.teardown, self.pool = controller, teardown, session_pool
        controller.bind_execution_context(capability.context)
        self.scope_key = capability.context.user_id + ":" + capability.context.thread_id
        if teardown.workspace_sessions is not None and teardown.workspace_sessions != (session_pool, self.scope_key):
            raise OwnershipRejected("Original workspace MCP owner scope required")
        session_pool.manage_scope(self.scope_key)
        teardown.workspace_sessions = (session_pool, self.scope_key)
        from deerflow_ecs_fleet.persistence.workspace_points import WorkspaceRequests

        self.requests = WorkspaceRequests()
        self.terminal = FleetWorkspaceTerminalParticipant(capability, controller=controller)
        self.accessor = None
        self.source_version = capability._guard._spec.workspace_manifest_ref
        self._callback_lock = asyncio.Lock()

    def bind_accessor(self, accessor):
        if self.accessor is not None and self.accessor is not accessor:
            raise OwnershipRejected("Original checkpoint accessor binding changed")
        self.accessor = accessor

    def boundary(self, config, *, key, kind, paths=(), status=None, error=None, stop_reason=None, task_status=None):
        import hashlib

        from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity, canonical

        checkpoint = config["configurable"]
        if checkpoint.get("checkpoint_ns") or checkpoint["thread_id"] != self.capability.context.thread_id:
            raise OwnershipRejected("Original root checkpoint configuration required")
        outcomes = {"success": "succeeded", "error": "failed", "interrupted": "cancelled", "timeout": "timed_out"}
        return WorkspaceBoundaryIdentity.from_context(
            self.capability.context,
            request_id=hashlib.sha256(canonical([self.capability.context.attempt_id, checkpoint["checkpoint_id"], kind, key])).hexdigest(),
            checkpoint_id=checkpoint["checkpoint_id"],
            kind=kind,
            publication_key=key,
            presented_paths=tuple(sorted(set(path.removeprefix("/mnt/user-data/") for path in paths))),
            source_workspace_version=self.source_version,
            desired_core_status=status,
            desired_task_status=task_status or outcomes.get(status),
            desired_placement_status=outcomes.get(status),
            error=error,
            stop_reason=stop_reason,
        )

    async def accept_partial(self, identity, candidate, *, barrier_epoch):
        import time

        deadline = self.controller.execution_deadline
        if deadline is None:
            raise OwnershipRejected("Original partial execution deadline required")
        async with asyncio.timeout(max(0, deadline - time.monotonic())):
            async with self.sf.begin() as session:
                await self._bound_sql(session, deadline)
                await self._validate(session, identity)
                await self.terminal.accept_partial(session, identity, candidate, barrier_epoch=barrier_epoch)
                await self._validate(session, identity)
            # Only the exact accepted pair's actual commit permits original
            # native writers and managed MCP owner/reconnect admission.
            self.pool.reopen_scope(self.scope_key, barrier_epoch=barrier_epoch)
            self.controller.reopen(barrier_epoch)
            self.source_version = candidate.manifest_id

    async def on_root_commit(self, config, metadata):
        from deerflow.runtime.execution.mutation_context import ExecutionWorkspaceFailure

        if not self.controller.pending_presentations:
            return
        if self.accessor is None:
            raise ExecutionWorkspaceFailure("Original checkpoint accessor is unavailable")
        try:
            async with self._callback_lock:
                snapshot = await self.accessor.aget(config)
                messages = snapshot.values.get("messages", [])
                ids = {message.id for message in messages if getattr(message, "type", None) == "tool" and message.content == "Successfully presented files"}
                for key, message_id, parent, paths in self.controller.pending_presentations:
                    if message_id not in ids:
                        continue
                    identity = self.boundary(config, key=key, kind="partial", paths=paths)
                    epoch = await self.publish(identity, ancestor=parent)
                    candidate = FleetWorkspaceNodeService._manifest(await self.wait_prepared(identity, barrier_epoch=epoch, deadline=self.controller.execution_deadline))
                    await self.accept_partial(identity, candidate, barrier_epoch=epoch)
                    self.controller.accepted_presentation(key)
        except BaseException as error:
            if isinstance(error, (asyncio.CancelledError, ExecutionWorkspaceFailure)):
                raise
            raise ExecutionWorkspaceFailure("Original partial checkpoint/workspace publication failed") from error

    async def prepare_terminal(self, record):
        import hashlib

        from deerflow_ecs_fleet.agent_workspace import canonical

        if self.accessor is None:
            raise OwnershipRejected("Original final checkpoint accessor required")
        # Actual final root is selected after original duration/title/rollback
        # mutations, materialized with the original full/delta compiled graph.
        snapshot = await self.accessor.aget({"configurable": {"thread_id": self.capability.context.thread_id, "checkpoint_ns": ""}})
        status = record.status.value
        key = hashlib.sha256(canonical([snapshot.config["configurable"]["checkpoint_id"], status, record.error, record.stop_reason])).hexdigest()
        human_input = self.controller.awaiting_human_input(snapshot)
        paused = status == "interrupted" and bool(snapshot.next or any(task.interrupts for task in snapshot.tasks) or human_input)
        requires_input = paused and (human_input or any(task.interrupts for task in snapshot.tasks))
        identity = self.boundary(
            snapshot.config,
            key=key,
            kind="paused" if paused else "final",
            paths=snapshot.values.get("artifacts", []),
            status=status,
            error=record.error,
            stop_reason=record.stop_reason,
            task_status="input_required" if requires_input else "paused" if paused else None,
        )
        epoch = await self.publish(identity)
        candidate = FleetWorkspaceNodeService._manifest(await self.wait_prepared(identity, barrier_epoch=epoch, deadline=self.teardown.budget.deadline))
        self.terminal.bind_prepared(identity, candidate, barrier_epoch=epoch)

    def _identity(self, identity):
        from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity

        context = self.capability.context
        if not isinstance(identity, WorkspaceBoundaryIdentity) or current_remote_mutation_context() != context or any(getattr(identity, field.name) != getattr(context, field.name) for field in fields(context)):
            raise OwnershipRejected("Original private workspace publication identity required")

    async def _validate(self, session, identity):
        self._identity(identity)
        run = await self.capability._guard.validate(_SessionCursor(session), thread_id=identity.thread_id, operation="workspace.publish")
        from deerflow_ecs_fleet.persistence.models import AgentTaskRow, NodeRow

        task = await session.get(AgentTaskRow, identity.agent_task_id)
        node = await session.get(NodeRow, identity.node_id)
        if task.cancel_requested_at is not None or run["cancel_action"] is not None or node.admin_state != "enabled":
            raise OwnershipRejected("Original workspace publication was stopped")

    @staticmethod
    async def _bound_sql(session, deadline):
        import time

        from sqlalchemy import text

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Original workspace execution deadline elapsed")
        milliseconds = str(max(1, int(remaining * 1000)))
        await session.execute(text("SELECT set_config('lock_timeout', :limit, true), set_config('statement_timeout', :limit, true)"), {"limit": milliseconds})

    async def publish(self, identity, *, ancestor=None):
        import time

        self._identity(identity)
        if identity.kind == "partial":
            deadline = self.controller.execution_deadline
            if deadline is None:
                raise OwnershipRejected("Original workspace execution deadline required")
            epoch = await self.controller.close_and_wait(deadline=deadline)
        else:
            # The same original final budget starts only at a final boundary.
            self.teardown.budget.start()
            deadline = self.teardown.budget.deadline
            try:
                epoch = await self.teardown.phase("workspace-publication-writers", lambda: self.controller.close_and_wait(deadline=deadline, final=True))
            except BaseException as error:
                if self.controller.unsettled:
                    self.teardown.retain_pending(error)
                raise
        self.pool.freeze_scope(self.scope_key, barrier_epoch=epoch)
        if identity.kind == "partial":
            await self.teardown.settle_workspace_sessions(deadline=deadline)
        else:
            await self.teardown.phase("workspace-mcp", lambda: self.teardown.settle_workspace_sessions(deadline=deadline))
        if identity.kind == "partial":
            try:
                await self.controller.stop_and_join_processes(deadline=deadline)
            except BaseException as error:
                if self.controller.unsettled:
                    self.teardown.retain_pending(error)
                raise
        # Node independently verifies zero children before any copy. No SQL lock is held during
        # actual writer or MCP owner settlement, and no point is accepted here.
        async with asyncio.timeout(max(0, deadline - time.monotonic())):
            async with self.sf.begin() as session:
                await self._bound_sql(session, deadline)
                await self._validate(session, identity)
                await _exact_root(session, identity, ancestor=ancestor)
                await self.requests.create(session, identity, barrier_epoch=epoch)
                await self._validate(session, identity)
        return epoch

    async def wait_prepared(self, identity, *, barrier_epoch, deadline):
        import time

        from deerflow_ecs_fleet.persistence.models import WorkspaceManifestRow

        self._identity(identity)
        if self.controller.barrier_epoch != barrier_epoch:
            raise OwnershipRejected("Original workspace barrier epoch changed")
        # Partial wait is bounded by the original execution deadline; it never
        # reads teardown.phase/budget getters. Cancellation leaves admission
        # closed and durable candidates unaccepted for the original owner.
        deadline = min(deadline, self.controller.execution_deadline)
        if identity.kind != "partial":
            deadline = min(deadline, self.teardown.budget.deadline)
        async with asyncio.timeout(max(0, deadline - time.monotonic())):
            while True:
                async with self.sf.begin() as session:
                    await self._bound_sql(session, deadline)
                    await self._validate(session, identity)
                    row = await self.requests._locked(session, identity, barrier_epoch=barrier_epoch)
                    candidate = await session.get(WorkspaceManifestRow, row.candidate_manifest_id) if row.state == "prepared" else None
                    await self._validate(session, identity)
                    if candidate is not None:
                        await session.flush()
                if candidate is not None:
                    # The producer stopped writers before publication; Node
                    # independently confirmed zero children. Host
                    # still positively reaps original Popen/capture handles.
                    await self.controller.join_processes(deadline=deadline)
                    return candidate
                await asyncio.sleep(min(0.05, max(0, deadline - time.monotonic())))


def _bounded_node_request(method):
    from functools import wraps

    @wraps(method)
    async def bounded(self, **kwargs):
        import time

        auth = {name: kwargs[name] for name in ("node_id", "node_session_id", "attempt_id")}
        deadline = await self._operation_deadline(auth)
        async with asyncio.timeout(max(0, deadline - time.monotonic())):
            return await method(self, **kwargs)

    return bounded


class FleetWorkspaceNodeService:
    """Original Node authentication/lease fences around prepared candidates only."""

    def __init__(self, session_factory, ownership):
        self.sf, self.ownership = session_factory, ownership
        from deerflow_ecs_fleet.persistence.workspace_points import WorkspaceRequests

        self.requests = WorkspaceRequests()

    async def _operation_deadline(self, auth):
        import time

        from sqlalchemy import text

        # This unlocked read only bounds waiting; it grants no mutation. The
        # actual operation still takes the original shared locks and auth fence.
        async with self.sf.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout='10s'"))
            await session.execute(text("SET LOCAL statement_timeout='10s'"))
            remaining = await session.scalar(
                text(
                    "SELECT extract(epoch FROM least(t.deadline,a.execution_deadline,a.lease_expires_at,r.lease_expires_at)-clock_timestamp()) "
                    "FROM fleet_attempts a JOIN fleet_run_placements p ON p.run_id=a.run_id "
                    "JOIN fleet_agent_tasks t ON t.id=p.agent_task_id JOIN runs r ON r.run_id=a.run_id "
                    "WHERE a.id=:attempt_id AND a.node_id=:node_id AND a.node_session_id=:node_session_id"
                ),
                auth,
            )
        if remaining is None:
            raise PermissionError("Original workspace execution unavailable")
        if remaining <= 0:
            raise OwnershipRejected("Original workspace lease/deadline elapsed")
        return time.monotonic() + min(10, float(remaining))

    async def _authenticate(self, session, auth, *, allow_idle=False):
        from deerflow_ecs_fleet.launch_spec import LaunchSpec
        from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity

        from .mutation import FleetMutationCapability

        attempt_auth = {key: value for key, value in auth.items() if key != "credential_id"}
        rows, _ = await self.ownership.attempts.authenticate(session, run_locker=self.ownership.lock_run, allow_terminal_run=allow_idle, **attempt_auth)
        task, run, placement, node, reservation, attempt = rows
        spec = LaunchSpec.model_validate(attempt.launch_spec["launch_spec"])
        original = ExecutionIdentity(node.id, attempt.node_session_id, task.id, placement.generation, attempt.id, run.owner_worker_id, attempt.token_hash)
        capability = FleetMutationCapability(original, spec)
        await capability._guard.validate(
            _SessionCursor(session),
            thread_id=spec.thread_id,
            operation="workspace.node.idle" if allow_idle else "workspace.node",
            allow_terminal=allow_idle,
        )
        now = await self._fresh_auth(session, capability, auth, allow_idle=allow_idle)
        if task.state == placement.state == "finishing":
            # Exact final authority permits a receipt, never a launch grant.
            return capability, None, min(task.deadline, attempt.execution_deadline, spec.execution_deadline), node.admin_state != "enabled"
        stopped = node.admin_state != "enabled" or task.cancel_requested_at is not None or run.cancel_action is not None
        return capability, self.ownership.grant(rows, now), min(task.deadline, attempt.execution_deadline, spec.execution_deadline), stopped

    @staticmethod
    async def _fresh_auth(session, capability, auth, *, allow_idle=False):
        from deerflow_ecs_fleet.persistence.models import CredentialRow

        credential = await session.get(CredentialRow, auth["credential_id"], with_for_update=True)
        # Credential waits and projection writes may cross either clock fence.
        await capability._guard.validate(
            _SessionCursor(session),
            thread_id=capability.context.thread_id,
            operation="workspace.node.idle" if allow_idle else "workspace.node",
            allow_terminal=allow_idle,
        )
        now = await session.scalar(select(func.clock_timestamp()))
        if credential is None or credential.node_id != capability.context.node_id or credential.revoked_at is not None or credential.expires_at <= now:
            raise PermissionError("Original Node credential no longer active")
        return now

    @staticmethod
    def _identity(row, context):
        from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity

        if row is None:
            raise ValueError("Original workspace request unavailable")
        values = {field.name: getattr(row, "id" if field.name == "request_id" else field.name) for field in fields(WorkspaceBoundaryIdentity)}
        values["presented_paths"] = tuple(values["presented_paths"])
        identity = WorkspaceBoundaryIdentity(**values)
        if any(getattr(identity, field.name) != getattr(context, field.name) for field in fields(context)) or row.request_digest != identity.request_digest:
            raise ValueError("Original workspace request execution conflicts")
        return identity

    @staticmethod
    def _manifest(row):
        import hashlib

        from deerflow_ecs_fleet.agent_workspace import WorkspaceManifest, canonical

        if row is None:
            return None
        names = ("schema_version", "request_digest", "user_id", "thread_id", "agent_task_id", "run_id", "generation", "attempt_id", "launch_spec_digest", "categories", "directories", "files", "total_bytes", "nas_prefix")
        private = {name: getattr(row, name) for name in ("node_id", "node_session_id", "owner_worker_id", "token_stamp", "process_ref")}
        return WorkspaceManifest(manifest_id=row.id, execution_digest=hashlib.sha256(canonical(private)).hexdigest(), **{name: getattr(row, name) for name in names})

    async def _projection(self, session, row, identity, grant):
        from deerflow_ecs_fleet.persistence.models import WorkspaceManifestRow

        processes = (
            (await session.execute(select(WorkspaceProcessRow).where(WorkspaceProcessRow.attempt_id == identity.attempt_id, WorkspaceProcessRow.state == "registered").limit(grant["execution_profile"]["pids_limit"] + 1))).scalars().all()
        )
        if len(processes) > grant["execution_profile"]["pids_limit"]:
            raise ValueError("Frozen workspace process registry exceeds bound")
        for process in processes:
            if any(getattr(process, field.name) != getattr(identity, field.name) for field in fields(self.ownership_context(identity))):
                raise ValueError("Registered workspace process execution conflicts")
        candidate = self._manifest(await session.get(WorkspaceManifestRow, row.candidate_manifest_id)) if row.candidate_manifest_id else None
        return {
            "identity": asdict(identity),
            "request_digest": identity.request_digest,
            "barrier_epoch": row.barrier_epoch,
            "state": row.state,
            "nonce": row.claim_nonce,
            "claim_lease_expires_at": row.claim_lease_expires_at.isoformat() if row.claim_lease_expires_at else None,
            "candidate": candidate.model_dump(mode="json") if candidate else None,
            "processes": [{field.name: getattr(process, field.name) for field in fields(OriginalToolProcess)} for process in processes],
            "grant": {name: grant[name] for name in ("kind", "node_id", "node_session_id", "run_id", "agent_task_id", "generation", "attempt_id", "owner_worker_id", "process_ref", "launch_spec", "execution_profile", "input_limits")},
        }

    @staticmethod
    def ownership_context(identity):
        from deerflow.runtime.execution.mutation_context import RemoteMutationContext

        return RemoteMutationContext(**{field.name: getattr(identity, field.name) for field in fields(RemoteMutationContext)})

    async def _accepted_idle(self, session, row, identity, *, nonce):
        """A receipt for this immutable point grants no copy or writer authority."""
        import hashlib
        from dataclasses import replace

        from deerflow_ecs_fleet.agent_workspace import canonical
        from deerflow_ecs_fleet.persistence.models import WorkspaceManifestRow, WorkspacePointRow
        from sqlalchemy import text

        if not isinstance(nonce, str) or len(nonce) != 64 or any(character not in "0123456789abcdef" for character in nonce) or row.claim_nonce != nonce:
            raise OwnershipRejected("Accepted workspace receipt nonce conflicts")
        point = await session.get(WorkspacePointRow, identity.request_id)
        expected = asdict(identity)
        expected["id"] = expected.pop("request_id")
        expected["request_id"] = identity.request_id
        expected.pop("presented_paths")
        expected.pop("source_workspace_version")
        expected.update(request_digest=identity.request_digest, manifest_id=row.candidate_manifest_id)
        if point is None or any(getattr(point, name) != value for name, value in expected.items()):
            raise OwnershipRejected("Accepted workspace receipt point conflicts")
        candidate = self._manifest(await session.get(WorkspaceManifestRow, point.manifest_id))
        private = {name: getattr(identity, name) for name in ("node_id", "node_session_id", "owner_worker_id", "token_stamp", "process_ref")}
        if (
            candidate is None
            or candidate.manifest_id != point.manifest_id
            or candidate.request_digest != identity.request_digest
            or candidate.execution_digest != hashlib.sha256(canonical(private)).hexdigest()
            or any(getattr(candidate, name) != getattr(identity, name) for name in ("user_id", "thread_id", "run_id", "agent_task_id", "generation", "attempt_id", "launch_spec_digest"))
        ):
            raise OwnershipRejected("Accepted workspace receipt candidate conflicts")
        head = await session.scalar(text("SELECT checkpoint_id FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1"), {"thread": identity.thread_id})
        # A partial receipt remains valid as this same run advances. Every root
        # in the ancestry retains the original private execution stamp.
        await _exact_root(session, replace(identity, checkpoint_id=head), ancestor=identity.checkpoint_id)

    @_bounded_node_request
    async def poll(self, **auth):
        from deerflow_ecs_fleet.persistence.models import WorkspaceRequestRow

        async with self.sf.begin() as session:
            capability, grant, _, stopped = await self._authenticate(session, auth, allow_idle=True)
            if stopped:
                return {"stop": True, "request": None}
            if grant is None:
                await self._fresh_auth(session, capability, auth, allow_idle=True)
                return {"stop": False, "request": None}
            row = (
                await session.execute(
                    select(WorkspaceRequestRow)
                    .where(WorkspaceRequestRow.attempt_id == auth["attempt_id"], WorkspaceRequestRow.state.in_(("requested", "sealing", "prepared")))
                    .order_by(WorkspaceRequestRow.created_at, WorkspaceRequestRow.id)
                    .limit(1)
                )
            ).scalar_one_or_none()
            projection = await self._projection(session, row, self._identity(row, capability.context), grant) if row else None
            await self._fresh_auth(session, capability, auth, allow_idle=True)
            return {"stop": False, "request": projection}

    @_bounded_node_request
    async def claim(self, *, request_id, request_digest, barrier_epoch, nonce, **auth):
        from deerflow_ecs_fleet.persistence.models import WorkspaceRequestRow

        async with self.sf.begin() as session:
            capability, grant, deadline, stopped = await self._authenticate(session, auth, allow_idle=True)
            if stopped:
                return {"stop": True, "request": None}
            identity = self._identity(await session.get(WorkspaceRequestRow, request_id), capability.context)
            if request_digest != identity.request_digest:
                raise ValueError("Original workspace request digest conflicts")
            row = await self.requests._locked(session, identity, barrier_epoch=barrier_epoch)
            if row.state == "accepted":
                await self._accepted_idle(session, row, identity, nonce=nonce)
                await self._fresh_auth(session, capability, auth, allow_idle=True)
                return {"stop": False, "request": None}
            if grant is None:
                raise OwnershipRejected("Final cleanup cannot claim new workspace work")
            row = await self.requests.claim(session, identity, nonce=nonce, barrier_epoch=barrier_epoch, deadline=deadline)
            projection = await self._projection(session, row, identity, grant)
            await self._fresh_auth(session, capability, auth, allow_idle=True)
            return {"stop": False, "request": projection}

    @_bounded_node_request
    async def prepared(self, *, request_id, request_digest, barrier_epoch, nonce, manifest, **auth):
        import hashlib

        from deerflow_ecs_fleet.agent_workspace import MAX_METADATA_BYTES, AgentWorkspaceVersions, WorkspaceManifest, canonical
        from deerflow_ecs_fleet.persistence.models import WorkspaceRequestRow
        from deerflow_ecs_fleet.workspace import NASWorkspace

        if len(canonical(manifest)) > MAX_METADATA_BYTES:
            raise ValueError("Workspace candidate metadata exceeds bound")
        candidate = WorkspaceManifest.model_validate(manifest)
        async with self.sf.begin() as session:
            capability, grant, _, stopped = await self._authenticate(session, auth)
            if stopped:
                return {"stop": True, "request": None}
            identity = self._identity(await session.get(WorkspaceRequestRow, request_id), capability.context)
            if request_digest != identity.request_digest:
                raise ValueError("Original workspace request digest conflicts")
            row = await self.requests._locked(session, identity, barrier_epoch=barrier_epoch)
            now = await session.scalar(select(func.clock_timestamp()))
            if row.state not in {"sealing", "prepared"} or row.claim_nonce != nonce or row.claim_lease_expires_at <= now:
                raise ValueError("Original workspace prepare claim expired or conflicts")
            private = {name: getattr(identity, name) for name in ("node_id", "node_session_id", "owner_worker_id", "token_stamp", "process_ref")}
            if (
                candidate.request_digest != identity.request_digest
                or candidate.execution_digest != hashlib.sha256(canonical(private)).hexdigest()
                or any(getattr(candidate, name) != getattr(identity, name) for name in ("user_id", "thread_id", "agent_task_id", "run_id", "generation", "attempt_id", "launch_spec_digest"))
            ):
                raise ValueError("Prepared candidate original identity conflicts")
            await self._fresh_auth(session, capability, auth)
        # No original SQL lock survives physical inventory/hash verification.
        versions = AgentWorkspaceVersions(
            NASWorkspace(self.ownership.config.nas_root, identity=self.ownership.config.nas_identity), max_input_bytes=grant["input_limits"]["max_input_bytes"], max_output_bytes=grant["execution_profile"]["max_output_bytes"]
        )
        verified = await asyncio.to_thread(versions.verify, candidate)
        async with self.sf.begin() as session:
            capability, grant, _, stopped = await self._authenticate(session, auth)
            if stopped:
                return {"stop": True, "request": None}
            fresh = self._identity(await session.get(WorkspaceRequestRow, request_id), capability.context)
            if fresh != identity:
                raise ValueError("Original workspace identity changed during candidate verification")
            row = await self.requests.prepared(session, fresh, nonce=nonce, barrier_epoch=barrier_epoch, manifest=verified)
            projection = await self._projection(session, row, fresh, grant)
            await self._fresh_auth(session, capability, auth)
            return {"stop": False, "request": projection}


class FleetWorkspaceTerminalParticipant:
    """Exact prepared pair joined to the original core terminal transaction.

    Binding occurs after physical candidate verification with no SQL locks.
    Acceptance never owns or commits a session.
    """

    def __init__(self, capability, *, controller):
        self.capability = capability
        self.controller = controller
        self.prepared = None
        from deerflow_ecs_fleet.persistence.workspace_points import WorkspaceRequests

        self.requests = WorkspaceRequests()

    def bind_prepared(self, identity, manifest, *, barrier_epoch):
        import hashlib

        from deerflow_ecs_fleet.agent_workspace import WorkspaceManifest, canonical

        context = self.capability.context
        candidate = WorkspaceManifest.model_validate(manifest)
        private = {name: getattr(context, name) for name in ("node_id", "node_session_id", "owner_worker_id", "token_stamp")}
        private["process_ref"] = "fleet-" + context.attempt_id
        if (
            any(getattr(identity, field.name) != getattr(context, field.name) for field in fields(context))
            or identity.kind not in {"final", "paused"}
            or candidate.request_digest != identity.request_digest
            or candidate.execution_digest != hashlib.sha256(canonical(private)).hexdigest()
            or any(getattr(candidate, name) != getattr(identity, name) for name in ("user_id", "thread_id", "agent_task_id", "run_id", "generation", "attempt_id", "launch_spec_digest"))
            or self.controller.barrier_epoch != barrier_epoch
            or not self.controller._closed
            or not self.controller._final
            or self.controller.unsettled
        ):
            raise OwnershipRejected("Exact final preparation identity rejected")
        self.prepared = (identity, candidate, barrier_epoch)

    async def _locked(self, session, *, run_id, status, error, stop_reason):

        if current_remote_mutation_context() != self.capability.context or self.prepared is None:
            raise OwnershipRejected("Original final preparation required")
        identity, candidate, epoch = self.prepared
        if (
            (run_id, status) != (identity.run_id, identity.desired_core_status)
            or (error is not None and error != identity.error)
            or (stop_reason is not None and stop_reason != identity.stop_reason)
            or self.controller.barrier_epoch != epoch
            or not self.controller._final
            or self.controller.unsettled
        ):
            raise OwnershipRejected("Original final preparation outcome rejected")
        run = await self.capability._guard.validate(_SessionCursor(session), thread_id=identity.thread_id, operation="workspace.accept", allow_terminal=True)
        row = await self.requests._locked(session, identity, barrier_epoch=epoch)
        if row.state not in {"prepared", "accepted"} or row.candidate_manifest_id != candidate.manifest_id:
            raise OwnershipRejected("Exact prepared final candidate required")
        from deerflow_ecs_fleet.persistence.models import WorkspaceManifestRow

        stored = FleetWorkspaceNodeService._manifest(await session.get(WorkspaceManifestRow, candidate.manifest_id))
        if stored != candidate:
            raise OwnershipRejected("Prepared final descriptor changed")
        # Last root includes duration, late title and rollback mutations; an
        # exact immutable candidate never authorizes a newer root head.
        await _exact_root(session, identity)
        return identity, candidate, row, run

    async def accept_partial(self, session, identity, candidate, *, barrier_epoch):
        from dataclasses import asdict

        from deerflow_ecs_fleet.agent_workspace import WorkspaceManifest
        from deerflow_ecs_fleet.persistence.models import AgentTaskRow, WorkspaceManifestRow, WorkspacePointRow

        context = self.capability.context
        candidate = WorkspaceManifest.model_validate(candidate)
        if (
            current_remote_mutation_context() != context
            or identity.kind != "partial"
            or any(getattr(identity, field.name) != getattr(context, field.name) for field in fields(context))
            or self.controller.barrier_epoch != barrier_epoch
            or not self.controller._closed
            or self.controller._final
            or self.controller.unsettled
        ):
            raise OwnershipRejected("Original settled partial boundary required")
        run = await self.capability._guard.validate(_SessionCursor(session), thread_id=identity.thread_id, operation="workspace.accept")
        if run["cancel_action"] is not None:
            raise OwnershipRejected("Cancelled execution cannot accept partial")
        request = await self.requests._locked(session, identity, barrier_epoch=barrier_epoch)
        if request.state not in {"prepared", "accepted"} or request.candidate_manifest_id != candidate.manifest_id or candidate.request_digest != identity.request_digest:
            raise OwnershipRejected("Exact prepared partial candidate required")
        stored = FleetWorkspaceNodeService._manifest(await session.get(WorkspaceManifestRow, candidate.manifest_id))
        if stored != candidate:
            raise OwnershipRejected("Prepared partial descriptor changed")
        await _exact_root(session, identity)
        values = asdict(identity)
        values["id"] = values.pop("request_id")
        values["request_id"] = identity.request_id
        values.pop("presented_paths")
        values.pop("source_workspace_version")
        values.update(request_digest=identity.request_digest, manifest_id=candidate.manifest_id)
        point = await session.get(WorkspacePointRow, identity.request_id, with_for_update=True)
        if point is None:
            if request.state != "prepared":
                raise OwnershipRejected("Accepted partial point is missing")
            session.add(WorkspacePointRow(**values))
        elif any(getattr(point, name) != value for name, value in values.items()):
            raise OwnershipRejected("Conflicting partial point rejected")
        task = await session.get(AgentTaskRow, identity.agent_task_id)
        if task.cancel_requested_at is not None:
            raise OwnershipRejected("Cancelled task cannot accept partial")
        previous = await session.get(WorkspacePointRow, task.accepted_workspace_point_id) if task.accepted_workspace_point_id else None
        if previous is not None and previous.kind != "partial":
            raise OwnershipRejected("Final point cannot be replaced by a stage")
        task.accepted_workspace_point_id = identity.request_id
        request.state = "accepted"
        await session.flush()
        await self.capability._guard.validate(_SessionCursor(session), thread_id=identity.thread_id, operation="workspace.accept")

    async def before_transition(self, session, **outcome):
        await self._locked(session, **outcome)

    async def after_transition(self, session, **outcome):
        from dataclasses import asdict

        from deerflow_ecs_fleet.persistence.models import AgentTaskRow, RunPlacementRow, WorkspacePointRow

        identity, candidate, request, run = await self._locked(session, **outcome)
        if run["status"] != identity.desired_core_status or (run["cancel_action"] is not None and run["status"] != "interrupted"):
            raise OwnershipRejected("Cancellation requires a newly prepared exact outcome")
        point = await session.get(WorkspacePointRow, identity.request_id, with_for_update=True)
        values = asdict(identity)
        values["id"] = values.pop("request_id")
        values["request_id"] = identity.request_id
        values.pop("presented_paths")
        values.pop("source_workspace_version")
        values.update(request_digest=identity.request_digest, manifest_id=candidate.manifest_id)
        if point is None:
            if request.state != "prepared":
                raise OwnershipRejected("Accepted final point is missing")
            point = WorkspacePointRow(**values)
            session.add(point)
        elif any(getattr(point, name) != value for name, value in values.items()):
            raise OwnershipRejected("Conflicting final point rejected")
        task = await session.get(AgentTaskRow, identity.agent_task_id)
        placement = await session.get(RunPlacementRow, identity.run_id)
        if task.accepted_workspace_point_id not in {None, identity.request_id}:
            previous = await session.get(WorkspacePointRow, task.accepted_workspace_point_id)
            if previous is None or previous.kind != "partial":
                raise OwnershipRejected("Conflicting accepted final point rejected")
        if placement.final_workspace_point_id not in {None, identity.request_id}:
            raise OwnershipRejected("Conflicting placement final point rejected")
        task.accepted_workspace_point_id = placement.final_workspace_point_id = identity.request_id
        task.state = placement.state = "finishing"
        request.state = "accepted"
        await session.flush()
        # Flush/unique/FK waits precede the original fresh wallclock fence.
        await self.capability._guard.validate(_SessionCursor(session), thread_id=identity.thread_id, operation="workspace.accept", allow_terminal=True)
