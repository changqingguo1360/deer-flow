"""Normal authenticated read transport for owned remote Agent tasks."""

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.gateway.authz import require_permission
from app.gateway.deps import get_current_user
from deerflow.utils.thread_id import ThreadId

router = APIRouter(prefix="/api/threads/{thread_id}/agent-tasks", tags=["agent-tasks"])


async def _read(request, thread_id, **kwargs):
    from app.fleet.runtime import fleet_runtime

    user_id = await get_current_user(request)
    if user_id is None:
        raise HTTPException(401, "Authentication required")
    runtime = fleet_runtime(request.app)
    if runtime is None:
        return []
    ownership = getattr(request.app.state, "fleet_ownership", None)
    if not runtime.ready or ownership is None:
        raise HTTPException(503, "Remote Agent task tracking unavailable")
    # Optional extension imports stay behind the installed runtime boundary.
    from app.fleet.task_summaries import FleetTaskSummaries

    return await FleetTaskSummaries(ownership.sf).read(user_id=user_id, thread_id=thread_id, **kwargs)


@router.get("")
@require_permission("runs", "read", owner_check=True)
async def list_agent_tasks(thread_id: ThreadId, request: Request, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0, le=10000)) -> list[dict]:
    return await _read(request, thread_id, limit=limit, offset=offset)


@router.get("/{task_id}")
@require_permission("runs", "read", owner_check=True)
async def get_agent_task(thread_id: ThreadId, task_id: str, request: Request) -> dict:
    records = await _read(request, thread_id, task_id=task_id, limit=1)
    if not records:
        raise HTTPException(404, "Agent task not found")
    return records[0]


class TaskOperationRequest(BaseModel):
    expected_generation: int = Field(ge=1)
    idempotency_key: str = Field(min_length=1, max_length=128)


@router.post("/{task_id}/resume")
@require_permission("runs", "create", owner_check=True)
async def resume_agent_task(thread_id: ThreadId, task_id: str, body: TaskOperationRequest, request: Request):
    from app.gateway.routers.thread_runs import RunCreateRequest, _record_to_response
    from app.gateway.services import start_run
    from deerflow.runtime.runs.manager import ConflictError

    ownership = getattr(request.app.state, "fleet_ownership", None)
    if ownership is None:
        raise HTTPException(503, "Remote Agent task tracking unavailable")
    from app.fleet.task_admission import owned_resume_backend

    try:
        backend = await owned_resume_backend(ownership, user_id=await get_current_user(request), thread_id=thread_id, task_id=task_id, expected_generation=body.expected_generation, idempotency_key=body.idempotency_key)
        record = await start_run(RunCreateRequest(input={"messages": []}), thread_id, request, execution_backend=backend, idempotency_key="fleet-resume:" + task_id + ":" + body.idempotency_key, require_existing_thread=True)
        return _record_to_response(record)
    except LookupError:
        raise HTTPException(404, "Agent task not found") from None
    except ConflictError as exc:
        raise HTTPException(409, str(exc)) from None


@router.post("/{task_id}/cancel")
@require_permission("runs", "cancel", owner_check=True)
async def cancel_agent_task(thread_id: ThreadId, task_id: str, body: TaskOperationRequest, request: Request):
    from deerflow.runtime.runs.manager import ConflictError

    ownership = getattr(request.app.state, "fleet_ownership", None)
    if ownership is None:
        raise HTTPException(503, "Remote Agent task tracking unavailable")
    from app.fleet.task_operations import cancel_owned_task

    try:
        return await cancel_owned_task(ownership, user_id=await get_current_user(request), thread_id=thread_id, task_id=task_id, expected_generation=body.expected_generation, idempotency_key=body.idempotency_key)
    except LookupError:
        raise HTTPException(404, "Agent task not found") from None
    except ConflictError as exc:
        raise HTTPException(409, str(exc)) from None
