"""Early host mutation gate; durable reservations still guard actual writes."""

from fastapi import HTTPException

from app.gateway.deps import get_current_user, get_run_manager
from deerflow.runtime.runs.manager import ConflictError


async def require_thread_mutation_admission(request, thread_id):
    manager = get_run_manager(request)
    guard = getattr(manager, "assert_thread_operation_allowed", None)
    if guard is None:
        return
    try:
        await guard(thread_id, user_id=await get_current_user(request))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
