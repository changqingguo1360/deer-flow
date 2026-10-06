"""Normal authenticated read transport for owned remote Agent tasks."""

from fastapi import APIRouter, HTTPException, Query, Request

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
