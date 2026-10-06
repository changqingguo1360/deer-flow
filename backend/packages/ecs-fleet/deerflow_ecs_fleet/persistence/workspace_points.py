"""Private C request/candidate helpers; the host fences the caller's same TX.

Copy and complete descriptor verification belong outside this transaction.
These helpers never accept a point or mutate a core run/placement/task.
"""

import hashlib
import re
from dataclasses import asdict
from datetime import timedelta

from sqlalchemy import func, select

from ..agent_workspace import MAX_METADATA_BYTES, WorkspaceBoundaryIdentity, WorkspaceManifest, canonical
from .models import WorkspaceManifestRow, WorkspaceRequestRow

__all__ = ["WorkspaceBoundaryIdentity", "WorkspaceManifest", "WorkspaceRequests"]


def _request_values(identity):
    if not isinstance(identity, WorkspaceBoundaryIdentity):
        raise ValueError("Original immutable workspace boundary required")
    values = asdict(identity)
    values["id"] = values.pop("request_id")
    values["presented_paths"] = list(identity.presented_paths)
    values["request_digest"] = identity.request_digest
    if len(canonical(values)) > MAX_METADATA_BYTES:
        raise ValueError("Workspace request metadata exceeds bound")
    return values


class WorkspaceRequests:
    async def _locked(self, session, identity, *, barrier_epoch):
        row = await session.get(WorkspaceRequestRow, identity.request_id, with_for_update=True)
        expected = _request_values(identity)
        if row is None or any(getattr(row, key) != value for key, value in expected.items()):
            raise ValueError("Original workspace request identity conflicts")
        if type(barrier_epoch) is not int or barrier_epoch <= 0 or row.barrier_epoch != barrier_epoch:
            raise ValueError("Original workspace barrier epoch conflicts")
        return row

    async def create(self, session, identity, *, barrier_epoch):
        values = _request_values(identity)
        if type(barrier_epoch) is not int or barrier_epoch <= 0:
            raise ValueError("Closed original workspace barrier required")
        row = await session.get(WorkspaceRequestRow, identity.request_id, with_for_update=True)
        if row is not None:
            return await self._locked(session, identity, barrier_epoch=barrier_epoch)
        row = WorkspaceRequestRow(**values, barrier_epoch=barrier_epoch)
        session.add(row)
        await session.flush()
        return row

    async def claim(self, session, identity, *, nonce, barrier_epoch, deadline):
        if not isinstance(nonce, str) or not re.fullmatch(r"[a-f0-9]{64}", nonce):
            raise ValueError("Invalid original workspace claim nonce")
        row = await self._locked(session, identity, barrier_epoch=barrier_epoch)
        now = await session.scalar(select(func.clock_timestamp()))
        if deadline <= now:
            raise ValueError("Original workspace execution deadline elapsed")
        if row.state in {"sealing", "prepared"}:
            if row.claim_nonce != nonce:
                raise ValueError("Original workspace claim nonce conflicts")
            if row.claim_lease_expires_at <= now:
                # Host has just fenced the same original execution/closed
                # barrier. A lost response can renew this exact nonce; it
                # cannot replace identity, epoch, or a prepared candidate.
                row.claim_lease_expires_at = min(deadline, now + timedelta(seconds=30))
                row.updated_at = now
                await session.flush()
            return row
        if row.state != "requested" or row.claim_nonce is not None:
            raise ValueError("Original workspace request cannot be claimed")
        row.state = "sealing"
        row.claim_nonce = nonce
        row.claim_lease_expires_at = min(deadline, now + timedelta(seconds=30))
        row.updated_at = now
        await session.flush()
        return row

    async def prepared(self, session, identity, *, nonce, barrier_epoch, manifest):
        row = await self._locked(session, identity, barrier_epoch=barrier_epoch)
        now = await session.scalar(select(func.clock_timestamp()))
        if row.state not in {"sealing", "prepared"} or row.claim_nonce != nonce or row.claim_lease_expires_at <= now:
            raise ValueError("Original workspace prepare claim rejected")
        manifest = WorkspaceManifest.model_validate(manifest)
        owner_fields = ("user_id", "thread_id", "agent_task_id", "run_id", "generation", "attempt_id", "launch_spec_digest")
        private = {name: getattr(identity, name) for name in ("node_id", "node_session_id", "owner_worker_id", "token_stamp", "process_ref")}
        if manifest.request_digest != identity.request_digest or manifest.execution_digest != hashlib.sha256(canonical(private)).hexdigest() or any(getattr(manifest, name) != getattr(identity, name) for name in owner_fields):
            raise ValueError("Prepared candidate original execution conflicts")
        payload = manifest.model_dump(mode="json")
        if len(canonical(payload)) > MAX_METADATA_BYTES:
            raise ValueError("Workspace candidate metadata exceeds bound")
        values = {name: getattr(identity, name) for name in (*owner_fields, *private)}
        values.update(
            id=manifest.manifest_id,
            request_digest=identity.request_digest,
            content_hash=manifest.manifest_id,
            schema_version=manifest.schema_version,
            categories=payload["categories"],
            directories=payload["directories"],
            files=payload["files"],
            total_bytes=manifest.total_bytes,
            nas_prefix=manifest.nas_prefix,
        )
        existing = await session.get(WorkspaceManifestRow, manifest.manifest_id)
        if existing is not None:
            if any(getattr(existing, key) != value for key, value in values.items()):
                raise ValueError("Existing prepared candidate descriptor conflicts")
        else:
            if row.state == "prepared":
                raise ValueError("Prepared candidate is missing durable descriptor")
            session.add(WorkspaceManifestRow(**values))
            await session.flush()
        if row.state == "prepared" and row.candidate_manifest_id != manifest.manifest_id:
            raise ValueError("Original workspace candidate cannot change")
        row.state = "prepared"
        row.candidate_manifest_id = manifest.manifest_id
        row.updated_at = now
        await session.flush()
        return row


async def accepted_final(session, *, task, run, placement, attempt):
    """Read exact immutable final authority; never infer it from core status.

    The caller already owns the original task/run/placement/node/ledger/attempt
    locks. This helper acquires no independent transaction and grants no writes.
    """
    from .models import WorkspacePointRow

    if not task.accepted_workspace_point_id or task.accepted_workspace_point_id != placement.final_workspace_point_id:
        return None
    point = await session.get(WorkspacePointRow, placement.final_workspace_point_id)
    if point is None or point.kind not in {"final", "paused"}:
        return None
    if task.id != placement.agent_task_id or task.current_run_id != run.run_id or task.generation != placement.generation:
        return None
    return await accepted_source_identity(session, point=point, run=run, placement=placement, attempt=attempt)


async def accepted_source_identity(session, *, point, run, placement, attempt, require_latest_checkpoint=True):
    """Immutable original proof; callers must separately authorize current lineage."""
    from sqlalchemy import text

    if point is None or point.kind not in {"final", "paused"} or placement.final_workspace_point_id != point.id or placement.active_attempt_id != attempt.id:
        return None
    expected = dict(
        user_id=run.user_id,
        thread_id=run.thread_id,
        run_id=run.run_id,
        agent_task_id=placement.agent_task_id,
        generation=placement.generation,
        attempt_id=attempt.id,
        node_id=attempt.node_id,
        node_session_id=attempt.node_session_id,
        token_stamp=attempt.token_hash,
        process_ref=attempt.process_ref,
        owner_worker_id=run.owner_worker_id,
        desired_core_status=run.status,
        error=run.error,
        stop_reason=run.stop_reason,
    )
    if any(getattr(point, name) != value for name, value in expected.items()):
        return None
    from ..launch_spec import LaunchSpec

    if point.launch_spec_digest != LaunchSpec.model_validate(attempt.launch_spec["launch_spec"]).payload_digest():
        return None
    if require_latest_checkpoint:
        checkpoint_query = text("SELECT checkpoint_id,metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1")
    else:
        # Historical receipt recovery reads the original accepted checkpoint;
        # a later human run must not invalidate or replace this old proof.
        checkpoint_query = text("SELECT checkpoint_id,metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint")
    root = (await session.execute(checkpoint_query, {"thread": run.thread_id, "checkpoint": point.checkpoint_id})).mappings().first()
    if root is None or root["checkpoint_id"] != point.checkpoint_id or root["metadata"].get("deerflow_execution_run_id") != run.run_id:
        return None
    request = await session.get(WorkspaceRequestRow, point.request_id)
    if request is None or request.state != "accepted" or request.candidate_manifest_id != point.manifest_id or request.request_digest != point.request_digest or request.barrier_epoch <= 0:
        return None
    return point
