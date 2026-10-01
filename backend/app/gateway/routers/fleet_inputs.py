"""Authenticated thread-owned uploads create immutable Fleet input versions."""

from fastapi import APIRouter, File, HTTPException, Request, UploadFile

from app.gateway.authz import require_permission
from app.gateway.deps import get_current_user
from app.gateway.fleet_auth import get_fleet_runtime
from deerflow.utils.thread_id import ThreadId

router = APIRouter(prefix="/api/threads/{thread_id}/fleet/inputs", tags=["fleet-inputs"])


async def input_runtime(request):
    user_id = await get_current_user(request)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    runtime = get_fleet_runtime(request.app)
    if getattr(runtime, "inputs", None) is None:
        raise HTTPException(status_code=503, detail="Fleet inputs unavailable")
    return runtime, user_id


@router.post("", status_code=201)
@require_permission("threads", "write", owner_check=True, require_existing=True)
async def register_inputs(thread_id: ThreadId, request: Request, files: list[UploadFile] = File(...)):
    runtime, user_id = await input_runtime(request)
    if not 1 <= len(files) <= 64 or any(not file.filename for file in files):
        raise HTTPException(status_code=422, detail="One to 64 named files required")
    try:
        return await runtime.inputs.register(user_id=user_id, thread_id=thread_id, files=[(file.filename, file.file) for file in files])
    except (ValueError, OSError):
        raise HTTPException(status_code=409, detail="Input registration not accepted") from None


@router.get("/{input_id}")
@require_permission("threads", "read", owner_check=True)
async def get_input(thread_id: ThreadId, input_id: str, request: Request):
    runtime, user_id = await input_runtime(request)
    try:
        return await runtime.inputs.get(input_id, user_id=user_id, thread_id=thread_id)
    except LookupError:
        raise HTTPException(status_code=404, detail="Input version not found") from None
