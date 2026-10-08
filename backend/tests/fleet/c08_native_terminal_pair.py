"""Native fixture composition through original fenced checkpoint/pair APIs.

This utility does not create points or edit private metadata directly. Tests
still own their original process, drain, cancellation, and lock assertions.
"""

import hashlib
import os
import time
from datetime import UTC, datetime


async def advance_original_root(item):
    from langgraph.checkpoint.base import empty_checkpoint

    from app.fleet.mutation import FleetMutationCapability
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    capability = item.runs._mutation_capability if hasattr(item, "runs") else FleetMutationCapability(item.identity, item.spec)
    current = await item.writer.aget_tuple(item.config)
    checkpoint = empty_checkpoint() | {
        "channel_values": current.checkpoint["channel_values"],
        "channel_versions": current.checkpoint["channel_versions"],
    }
    with remote_mutation_scope(capability.context):
        item.config = await item.writer.aput(current.config, checkpoint, {"source": "loop", "step": 1, "parents": {}}, {})
    return item.config


class NativeTerminalPreparation:
    def __init__(self, item, directory, *, controller=None, cancellation=False):
        from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions
        from deerflow_ecs_fleet.workspace import NASWorkspace

        from app.fleet.workspace import FleetWorkspaceTerminalParticipant
        from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController

        self.item = item
        self.cancellation = cancellation
        self.capability = item.runs._mutation_capability
        self.controller = controller if controller is not None else WorkspaceWriterController()
        # Native fixture composition only; installed cumulative-120 ownership
        # remains a separate runtime gate. Execution keeps its frozen deadline.
        original = item.spec.execution_deadline
        self.execution_deadline = time.monotonic() + (original - datetime.now(UTC)).total_seconds()
        self.cleanup_deadline = None
        self.controller.execution_deadline = self.execution_deadline
        self.terminal = FleetWorkspaceTerminalParticipant(self.capability, controller=self.controller)
        item.runs._terminal_participant = self.terminal
        self.source = directory / "source"
        nas = directory / "nas"
        self.source.mkdir(parents=True)
        nas.mkdir()
        (nas / ".deerflow-fleet-root").write_text("native-terminal-fixture\n")
        for category in ("workspace", "uploads", "outputs"):
            (self.source / category).mkdir()
        self.versions = AgentWorkspaceVersions(NASWorkspace(nas, identity="native-terminal-fixture"), max_input_bytes=1024, max_output_bytes=1024)

    async def __call__(self, record):
        from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity, canonical
        from deerflow_ecs_fleet.persistence.workspace_points import WorkspaceRequests

        from deerflow.runtime.execution.mutation_context import remote_mutation_scope

        item = self.item
        if self.cleanup_deadline is None:
            self.cleanup_deadline = time.monotonic() + 120
        deadline = min(self.execution_deadline, self.cleanup_deadline)
        latest = await item.writer.aget_tuple({"configurable": {"thread_id": item.spec.thread_id, "checkpoint_ns": ""}})
        # Actual run_agent graph writes already carry its original private stamp.
        # A raw baseline fixture needs a real original FencedSaver write first.
        if latest.metadata.get("deerflow_execution_run_id") != item.spec.run_id:
            item.config = latest.config
            await advance_original_root(item)
            latest = await item.writer.aget_tuple(item.config)
        status = record.status.value if hasattr(record.status, "value") else record.status
        outcome = {"success": "succeeded", "interrupted": "cancelled", "error": "failed", "timeout": "timed_out"}[status]
        with remote_mutation_scope(self.capability.context), self.capability.cancellation_settlement_scope():
            epoch = await self.controller.close_and_wait(deadline=deadline, final=True)
            key = hashlib.sha256(canonical([latest.config["configurable"]["checkpoint_id"], status, getattr(record, "error", None), getattr(record, "stop_reason", None)])).hexdigest()
            identity = WorkspaceBoundaryIdentity.from_context(
                self.capability.context,
                request_id=key,
                checkpoint_id=latest.config["configurable"]["checkpoint_id"],
                kind="paused" if self.cancellation and status == "interrupted" else "final",
                publication_key=key,
                presented_paths=(),
                source_workspace_version="initial",
                desired_core_status=status,
                desired_task_status="paused" if self.cancellation and status == "interrupted" else outcome,
                desired_placement_status="cancelled" if self.cancellation and status == "interrupted" else outcome,
                error=getattr(record, "error", None),
                stop_reason=getattr(record, "stop_reason", None),
            )
            fd = os.open(self.source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                candidate = self.versions.verify(self.versions.seal(identity, fd))
            finally:
                os.close(fd)
            requests = WorkspaceRequests()
            async with item.env[1].begin() as session:
                await requests.create(session, identity, barrier_epoch=epoch)
                await requests.claim(session, identity, nonce="e" * 64, barrier_epoch=epoch, deadline=datetime.now(UTC) + __import__("datetime").timedelta(seconds=max(0, deadline - time.monotonic())))
                await requests.prepared(session, identity, nonce="e" * 64, barrier_epoch=epoch, manifest=candidate)
            self.terminal.bind_prepared(identity, candidate, barrier_epoch=epoch)


async def assert_original_terminal_pair(item):
    """Read exact durable pair/stamp/held capacity after the original transition."""
    from sqlalchemy import text

    async with item.engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT w.id,w.checkpoint_id,w.manifest_id,w.desired_core_status,r.status,q.state AS request_state,q.checkpoint_id AS "
                        "prepared_checkpoint,q.candidate_manifest_id,t.accepted_workspace_point_id,p.final_workspace_point_id,t.state AS task_state,p.state AS placement_state,a.stopped_at,res.state AS "
                        "reservation_state FROM fleet_workspace_points w JOIN fleet_workspace_requests q ON q.id=w.request_id JOIN fleet_agent_tasks t ON t.id=w.agent_task_id JOIN fleet_run_placements p ON "
                        "p.run_id=w.run_id JOIN runs r ON r.run_id=w.run_id JOIN fleet_attempts a ON a.id=w.attempt_id JOIN fleet_reservations res ON res.attempt_id=a.id WHERE w.run_id=:run"
                    ),
                    {"run": item.spec.run_id},
                )
            )
            .mappings()
            .one()
        )
        assert row["id"] == row["accepted_workspace_point_id"] == row["final_workspace_point_id"]
        assert row["checkpoint_id"] == row["prepared_checkpoint"]
        assert row["manifest_id"] == row["candidate_manifest_id"]
        assert row["request_state"] == "accepted"
        assert row["desired_core_status"] == row["status"]
        assert row["task_state"] == row["placement_state"] == "finishing"
        assert row["stopped_at"] is None and row["reservation_state"] in {"reserved", "active"}
        metadata = await conn.scalar(text("SELECT metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"), {"thread": item.spec.thread_id, "checkpoint": row["checkpoint_id"]})
        assert metadata["deerflow_execution_run_id"] == item.spec.run_id
