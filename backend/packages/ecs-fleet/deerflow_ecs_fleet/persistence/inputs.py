"""Immutable version identities are owner/thread scoped and pinned into claims."""

import asyncio
import json
from uuid import uuid4

from sqlalchemy import select

from .models import ArtifactManifestRow, InputManifestRow

MAX_PINNED_METADATA_BYTES = 256 * 1024


def references(spec):
    return list(dict.fromkeys([*spec.input_manifests, *([spec.code_artifact_id] if spec.code_artifact_id else [])]))


async def resolve_inputs(session, *, user_id, thread_id, spec, max_bytes):
    rows = []
    total = 0
    for identity in references(spec):
        row = (await session.execute(select(InputManifestRow).where(InputManifestRow.id == identity, InputManifestRow.user_id == user_id, InputManifestRow.thread_id == thread_id))).scalar_one_or_none()
        if row is None:
            row = (await session.execute(select(ArtifactManifestRow).where(ArtifactManifestRow.id == identity, ArtifactManifestRow.user_id == user_id, ArtifactManifestRow.thread_id == thread_id))).scalar_one_or_none()
        if row is None:
            raise PermissionError("Input version unavailable")
        total += row.total_bytes
        if total > max_bytes:
            raise ValueError("Job input byte budget exceeded")
        rows.append({"id": row.id, "schema_version": 1, "output_prefix": row.output_prefix, "files": row.files, "total_bytes": row.total_bytes})
    if len(json.dumps(rows, separators=(",", ":")).encode()) > MAX_PINNED_METADATA_BYTES:
        raise ValueError("Pinned input metadata budget exceeded")
    return rows


class InputManifests:
    def __init__(self, session_factory, *, workspace, config):
        self.sf = session_factory
        self.workspace = workspace
        self.config = config

    async def register(self, *, user_id, thread_id, files):
        if not self.config.jobs_enabled:
            raise ValueError("New Fleet input registration disabled")
        identity = str(uuid4())
        manifest = await asyncio.to_thread(self.workspace.publish_input, user_id, thread_id, identity, files, max_bytes=self.config.max_input_bytes)
        if len(json.dumps(manifest).encode()) > MAX_PINNED_METADATA_BYTES:
            raise ValueError("Input metadata budget exceeded")
        async with self.sf.begin() as session:
            session.add(InputManifestRow(id=identity, user_id=user_id, thread_id=thread_id, output_prefix=manifest["output_prefix"], files=manifest["files"], total_bytes=manifest["total_bytes"]))
        return {"id": identity, "files": manifest["files"], "total_bytes": manifest["total_bytes"]}

    async def get(self, identity, *, user_id, thread_id):
        async with self.sf() as session:
            row = (await session.execute(select(InputManifestRow).where(InputManifestRow.id == identity, InputManifestRow.user_id == user_id, InputManifestRow.thread_id == thread_id))).scalar_one_or_none()
            if row is None:
                raise LookupError("Input version unavailable")
            return {"id": row.id, "files": row.files, "total_bytes": row.total_bytes}
