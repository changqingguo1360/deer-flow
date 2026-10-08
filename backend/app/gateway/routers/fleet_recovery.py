"""Session administrators can review and close stopped uncertain executions."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.deps import get_current_user, require_admin_user
from app.gateway.fleet_auth import get_fleet_runtime

router = APIRouter(prefix="/api/fleet/recovery/jobs", tags=["fleet-recovery"])


class ResolveStoppedJob(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_attempt_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$")
    note: str = Field(min_length=1, max_length=1000)
    side_effects_reviewed: bool = Field(strict=True)

    @field_validator("side_effects_reviewed")
    @classmethod
    def reviewed(cls, value):
        if value is not True:
            raise ValueError("Explicit side-effect review required")
        return value

    @field_validator("note")
    @classmethod
    def meaningful_note(cls, value):
        if not value.strip():
            raise ValueError("Operator review note required")
        return value


async def recovery_runtime(request):
    if getattr(request.state, "auth_source", None) != AUTH_SOURCE_SESSION:
        raise HTTPException(status_code=403, detail="Administrator session required")
    await require_admin_user(request, detail="Fleet reconciliation requires an administrator")
    operator_id = await get_current_user(request)
    if operator_id is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    runtime = get_fleet_runtime(request.app)
    if getattr(runtime, "recovery", None) is None:
        raise HTTPException(status_code=503, detail="Fleet reconciliation unavailable")
    return runtime.recovery, operator_id


@router.get("")
async def list_unresolved(request: Request, limit: Annotated[int, Query(ge=1, le=100)] = 100, offset: Annotated[int, Query(ge=0, le=1_000_000)] = 0):
    recovery, _ = await recovery_runtime(request)
    return {"items": await recovery.list_unresolved(limit=limit, offset=offset)}


@router.post("/{job_id}/resolve")
async def resolve_stopped_job(job_id: str, body: ResolveStoppedJob, request: Request):
    recovery, operator_id = await recovery_runtime(request)
    try:
        return await recovery.resolve(job_id=job_id, operator_id=operator_id, **body.model_dump())
    except LookupError:
        raise HTTPException(status_code=404, detail="Job not found") from None
    except (ValueError, PermissionError):
        raise HTTPException(status_code=409, detail="Execution cannot be reconciled") from None


@router.get("/{job_id}/events")
async def get_recovery_events(job_id: str, request: Request):
    recovery, _ = await recovery_runtime(request)
    try:
        return {"items": await recovery.events(job_id)}
    except LookupError:
        raise HTTPException(status_code=404, detail="Job not found") from None
