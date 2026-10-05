"""Owner-scoped immutable accepted C points, separate from B job artifacts."""

import asyncio
import os
import shutil
from pathlib import Path

from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions
from deerflow_ecs_fleet.config import ExecutionProfile
from deerflow_ecs_fleet.persistence.models import AgentTaskRow, WorkspaceManifestRow, WorkspacePointRow, WorkspaceRequestRow
from deerflow_ecs_fleet.workspace import NASWorkspace
from sqlalchemy import select, text

from deerflow.persistence.run.model import ThreadExecutionBindingRow

from .workspace import FleetWorkspaceNodeService


async def _settle_owned(task):
    """Retain the worker until completion despite repeated request cancellation."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
    return task.result()


class FleetWorkspaceFiles:
    def __init__(self, session_factory, config):
        self.sf = session_factory
        self.versions = AgentWorkspaceVersions(
            NASWorkspace(config.nas_root, identity=config.nas_identity),
            max_input_bytes=config.max_input_bytes,
            max_output_bytes=max((p.max_output_bytes for p in config.profiles.values()), default=ExecutionProfile.model_fields["max_output_bytes"].default),
        )

    async def _accepted_manifest(self, session, point):
        request = await session.get(WorkspaceRequestRow, point.request_id)
        descriptor = await session.get(WorkspaceManifestRow, point.manifest_id)
        if descriptor is None:
            raise ValueError("Accepted workspace manifest is missing")
        manifest = FleetWorkspaceNodeService._manifest(descriptor)
        if request is None or request.state != "accepted" or request.candidate_manifest_id != point.manifest_id or request.request_digest != point.request_digest or manifest is None:
            raise ValueError("Accepted workspace descriptor is incomplete")
        if any(getattr(manifest, name) != getattr(point, name) for name in ("user_id", "thread_id", "run_id", "agent_task_id", "generation", "attempt_id", "launch_spec_digest", "request_digest")):
            raise ValueError("Accepted workspace descriptor identity conflicts")
        import hashlib

        from deerflow_ecs_fleet.agent_workspace import canonical

        private = {name: getattr(point, name) for name in ("node_id", "node_session_id", "owner_worker_id", "token_stamp", "process_ref")}
        if manifest.execution_digest != hashlib.sha256(canonical(private)).hexdigest():
            raise ValueError("Accepted workspace original execution digest conflicts")
        root = await session.scalar(text("SELECT metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"), {"thread": point.thread_id, "checkpoint": point.checkpoint_id})
        if root is None or root.get("deerflow_execution_run_id") != point.run_id:
            raise ValueError("Accepted checkpoint original execution conflicts")
        return manifest

    async def selected(self, *, user_id, thread_id, point_id=None, manifest_id=None, checkpoint_id=None):
        async with self.sf() as session:
            binding = await session.get(ThreadExecutionBindingRow, (user_id, thread_id))
            origin = binding.source_workspace if binding is not None else None
            own = await session.scalar(select(WorkspacePointRow.id).where(WorkspacePointRow.user_id == user_id, WorkspacePointRow.thread_id == thread_id).limit(1))
            if origin and (point_id == origin["point_id"] or manifest_id == origin["manifest_id"] or (checkpoint_id == origin["source_checkpoint_id"] and point_id is None and manifest_id is None) or own is None):
                if checkpoint_id not in (None, origin["source_checkpoint_id"]) or point_id not in (None, origin["point_id"]) or manifest_id not in (None, origin["manifest_id"]):
                    raise LookupError("Branch origin selection conflicts")
                thread_id = origin["source_thread_id"]
                point_id = origin["point_id"]
            query = select(WorkspacePointRow).where(WorkspacePointRow.user_id == user_id, WorkspacePointRow.thread_id == thread_id)
            if point_id is not None:
                query = query.where(WorkspacePointRow.id == point_id)
            elif checkpoint_id is not None:
                query = query.where(WorkspacePointRow.checkpoint_id == checkpoint_id).order_by(WorkspacePointRow.accepted_at.desc()).limit(1)
            elif manifest_id is not None:
                query = query.where(WorkspacePointRow.manifest_id == manifest_id)
            else:
                query = query.join(AgentTaskRow, AgentTaskRow.accepted_workspace_point_id == WorkspacePointRow.id).order_by(AgentTaskRow.created_at.desc()).limit(1)
            point = (await session.execute(query)).scalar_one_or_none()
            if point is None:
                raise LookupError("Accepted workspace point not found")
            if checkpoint_id not in (None, point.checkpoint_id) or manifest_id not in (None, point.manifest_id):
                raise LookupError("Accepted workspace selector combination conflicts")
            manifest = await self._accepted_manifest(session, point)
            metadata = dict(
                source_thread_id=point.thread_id,
                point_id=point.id,
                manifest_id=point.manifest_id,
                checkpoint_id=point.checkpoint_id,
                partial=point.kind != "final",
                files=[item.model_dump() for item in manifest.files],
                total_bytes=manifest.total_bytes,
            )
            return metadata, manifest

    async def metadata(self, **identity):
        metadata, manifest = await self.selected(**identity)
        await asyncio.to_thread(self.versions.verify, manifest)
        return metadata

    async def open(self, relative_path, **identity):
        metadata, manifest = await self.selected(**identity)
        pending = asyncio.create_task(asyncio.to_thread(self.versions.open_verified_output, manifest, relative_path))
        try:
            file, item = await asyncio.shield(pending)
        except asyncio.CancelledError:
            try:
                file, _ = await _settle_owned(pending)
            except BaseException:
                pass
            else:
                await _settle_owned(asyncio.create_task(asyncio.to_thread(file.close)))
            raise
        return file, item, metadata

    async def prepare_branch(self, *, user_id, parent_thread_id, thread_id, checkpoint_id):
        async with self.sf() as session:
            parent = await session.get(ThreadExecutionBindingRow, (user_id, parent_thread_id))
            inherited = parent.source_workspace if parent is not None else None
            target_origin = inherited if inherited and checkpoint_id == inherited.get("target_checkpoint_id") else None
        source_thread = target_origin["source_thread_id"] if target_origin else parent_thread_id
        source_checkpoint = target_origin["source_checkpoint_id"] if target_origin else checkpoint_id
        metadata, manifest = await self.selected(user_id=user_id, thread_id=source_thread, checkpoint_id=source_checkpoint, point_id=target_origin["point_id"] if target_origin else None)
        if metadata["checkpoint_id"] != source_checkpoint or manifest.thread_id != source_thread:
            raise ValueError("Branch accepted checkpoint identity conflicts")
        origin = dict(point_id=metadata["point_id"], manifest_id=metadata["manifest_id"], source_thread_id=source_thread, source_checkpoint_id=source_checkpoint, parent_checkpoint_id=checkpoint_id)
        async with self.sf.begin() as session:
            parent = await session.get(ThreadExecutionBindingRow, (user_id, parent_thread_id), with_for_update=True)
            if parent is None or parent.backend != "fleet" or parent.recovery_required:
                raise ValueError("Branch parent routing conflicts")
            if target_origin:
                if parent.source_workspace != target_origin:
                    raise ValueError("Branch original source selection changed")
                target = await session.scalar(text("SELECT metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"), {"thread": parent_thread_id, "checkpoint": checkpoint_id})
                if target is None or target.get("branch_parent_thread_id") != parent.parent_thread_id or target.get("branch_parent_checkpoint_id") != target_origin.get("parent_checkpoint_id", source_checkpoint):
                    raise ValueError("Branch target checkpoint ancestry conflicts")
            seen = {thread_id}
            current = parent_thread_id
            for _ in range(64):
                if current in seen:
                    raise ValueError("Branch routing ancestry is cyclic")
                seen.add(current)
                ancestor = await session.get(ThreadExecutionBindingRow, (user_id, current))
                if ancestor is None or ancestor.backend != "fleet" or ancestor.recovery_required:
                    raise ValueError("Branch routing ancestry owner conflicts")
                if ancestor.parent_thread_id is None:
                    break
                mapping = ancestor.source_workspace
                required = ("point_id", "manifest_id", "source_thread_id", "source_checkpoint_id", "parent_checkpoint_id", "target_checkpoint_id")
                if not isinstance(mapping, dict) or any(not isinstance(mapping.get(key), str) or not mapping[key] for key in required):
                    raise ValueError("Branch ancestor source mapping is incomplete")
                point = await session.get(WorkspacePointRow, mapping["point_id"])
                request = await session.get(WorkspaceRequestRow, point.request_id) if point is not None else None
                if (
                    point is None
                    or point.user_id != user_id
                    or (point.thread_id, point.checkpoint_id, point.manifest_id) != (mapping["source_thread_id"], mapping["source_checkpoint_id"], mapping["manifest_id"])
                    or request is None
                    or request.state != "accepted"
                    or request.candidate_manifest_id != point.manifest_id
                    or request.request_digest != point.request_digest
                ):
                    raise ValueError("Branch ancestor accepted source conflicts")
                # Every intermediate origin uses the same full accepted
                # descriptor/private execution proof as a direct file read.
                await self._accepted_manifest(session, point)
                source_root = await session.scalar(text("SELECT metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"), {"thread": point.thread_id, "checkpoint": point.checkpoint_id})
                target_root = await session.scalar(text("SELECT metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"), {"thread": current, "checkpoint": mapping["target_checkpoint_id"]})
                if (
                    source_root is None
                    or source_root.get("deerflow_execution_run_id") != point.run_id
                    or target_root is None
                    or (target_root.get("branch_parent_thread_id"), target_root.get("branch_parent_checkpoint_id")) != (ancestor.parent_thread_id, mapping["parent_checkpoint_id"])
                ):
                    raise ValueError("Branch ancestor checkpoint provenance conflicts")
                parent_binding = await session.get(ThreadExecutionBindingRow, (user_id, ancestor.parent_thread_id))
                if parent_binding is None or parent_binding.backend != "fleet" or parent_binding.recovery_required:
                    raise ValueError("Branch ancestor parent owner conflicts")
                if (point.thread_id, point.checkpoint_id) != (ancestor.parent_thread_id, mapping["parent_checkpoint_id"]):
                    parent_mapping = parent_binding.source_workspace
                    if (
                        not isinstance(parent_mapping, dict)
                        or parent_mapping.get("target_checkpoint_id") != mapping["parent_checkpoint_id"]
                        or any(parent_mapping.get(key) != mapping[key] for key in ("point_id", "manifest_id", "source_thread_id", "source_checkpoint_id"))
                    ):
                        raise ValueError("Branch ancestor parent source mapping conflicts")
                current = ancestor.parent_thread_id
            else:
                raise ValueError("Branch routing ancestry exceeds its bound")
            root = await session.scalar(text("SELECT metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"), {"thread": source_thread, "checkpoint": source_checkpoint})
            if root is None or root.get("deerflow_execution_run_id") != manifest.run_id:
                raise ValueError("Branch source checkpoint execution conflicts")
            session.add(ThreadExecutionBindingRow(user_id=user_id, thread_id=thread_id, backend="fleet", parent_thread_id=parent_thread_id, source_workspace=origin, recovery_required=True))
        return manifest

    async def remove_branch(self, destination):
        import anyio

        # HTTP middleware cancellation scopes may keep cancellation active.
        # Own the physical cleanup task through both scope and repeated raw
        # cancellation before the request can propagate its original failure.
        pending = asyncio.create_task(asyncio.to_thread(shutil.rmtree, destination, ignore_errors=True))
        with anyio.CancelScope(shield=True):
            await _settle_owned(pending)

    async def restore_branch(self, manifest, destination):
        destination = Path(destination)

        def restore():
            destination.mkdir(mode=0o700, parents=True, exist_ok=False)
            fd = os.open(destination, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                self.versions.restore(manifest, fd)
            except BaseException:
                shutil.rmtree(destination)
                raise
            finally:
                os.close(fd)

        pending = asyncio.create_task(asyncio.to_thread(restore))
        try:
            await asyncio.shield(pending)
        except asyncio.CancelledError:
            # Keep ownership through copy settlement and cleanup; no detached
            # callback or second cancellation can leave a usable child clone.
            try:
                await _settle_owned(pending)
            except BaseException:
                pass
            await _settle_owned(asyncio.create_task(asyncio.to_thread(shutil.rmtree, destination, ignore_errors=True)))
            raise

    async def finish_branch(self, *, user_id, thread_id, checkpoint_id):
        if not checkpoint_id:
            raise ValueError("Branch checkpoint required")
        async with self.sf.begin() as session:
            binding = await session.get(ThreadExecutionBindingRow, (user_id, thread_id), with_for_update=True)
            if binding is None or not binding.recovery_required or binding.backend != "fleet":
                raise ValueError("Branch preparation identity conflicts")
            binding.source_workspace = binding.source_workspace | {"target_checkpoint_id": checkpoint_id}
            binding.recovery_required = False
