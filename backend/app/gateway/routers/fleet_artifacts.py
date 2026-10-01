"""Owner/thread-scoped access to accepted immutable Fleet artifacts."""

import asyncio
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from app.gateway.authz import require_permission
from app.gateway.deps import get_current_user
from app.gateway.fleet_auth import get_fleet_runtime
from deerflow.utils.thread_id import ThreadId

router = APIRouter(prefix="/api/threads/{thread_id}/fleet/manifests", tags=["fleet-artifacts"])


async def owned_manifest(request, thread_id, manifest_id):
    user_id = await get_current_user(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    runtime = get_fleet_runtime(request.app)
    if runtime.manifests is None:
        raise HTTPException(status_code=503, detail="Fleet artifacts unavailable")
    try:
        row = await runtime.manifests.get(manifest_id, user_id=user_id, thread_id=thread_id)
    except LookupError:
        raise HTTPException(status_code=404, detail="Artifact manifest not found") from None
    return runtime, user_id, row


@router.get("/{manifest_id}")
@require_permission("threads", "read", owner_check=True)
async def get_manifest(thread_id: ThreadId, manifest_id: str, request: Request):
    _, _, row = await owned_manifest(request, thread_id, manifest_id)
    return {key: row[key] for key in ("id", "files", "total_bytes", "sealed_at")}


@router.get("/{manifest_id}/files/{relative_path:path}")
@require_permission("threads", "read", owner_check=True)
async def download_artifact(thread_id: ThreadId, manifest_id: str, relative_path: str, request: Request):
    runtime, user_id, row = await owned_manifest(request, thread_id, manifest_id)
    metadata = next((item for item in row["files"] if item["path"] == relative_path), None)
    if metadata is None:
        raise HTTPException(status_code=404, detail="Artifact not found")
    try:
        file = await runtime.manifests.open_artifact(manifest_id, relative_path, user_id=user_id, thread_id=thread_id)
    except (LookupError, ValueError, OSError):
        raise HTTPException(status_code=404, detail="Artifact not found") from None

    async def chunks():
        try:
            while chunk := await asyncio.to_thread(file.read, 65536):
                yield chunk
        finally:
            await asyncio.to_thread(file.close)

    headers = {"Content-Length": str(metadata["size"]), "Content-Disposition": "attachment; filename*=UTF-8''" + quote(relative_path.rsplit("/", 1)[-1], safe=""), "X-Content-Type-Options": "nosniff", "Cache-Control": "private, no-store"}
    return StreamingResponse(chunks(), media_type="application/octet-stream", headers=headers, background=BackgroundTask(file.close))
