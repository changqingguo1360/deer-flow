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
        from app.gateway.auth_disabled import AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_PAT, AUTH_SOURCE_SESSION

        participant = None
        if getattr(request.state, "auth_source", None) in {AUTH_SOURCE_SESSION, AUTH_SOURCE_PAT, AUTH_SOURCE_AUTH_DISABLED} and getattr(request.app.state, "fleet_ownership", None) is not None:
            from app.fleet.task_admission import FleetMutationObservation

            participant = FleetMutationObservation()
        await guard(thread_id, user_id=await get_current_user(request), **({"participant": participant} if participant is not None else {}))
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
