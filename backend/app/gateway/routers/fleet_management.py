"""Machine management requires an actual administrator session and normal CSRF."""

from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.deps import get_current_user, require_admin_user
from app.gateway.fleet_auth import get_fleet_runtime
from app.gateway.internal_auth import INTERNAL_AUTH_HEADER_NAME

router = APIRouter(prefix="/api/fleet/machines", tags=["fleet-management"])
IDENTITY = r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$"


class RegisterMachine(BaseModel):
    model_config = ConfigDict(extra="forbid")
    node_id: str = Field(pattern=IDENTITY)
    name: str = Field(pattern=IDENTITY)
    cpu_millis: int = Field(strict=True, ge=1, le=1_000_000)
    memory_mib: int = Field(strict=True, ge=1, le=4_194_304)
    profile_allowlist: list[str] = Field(min_length=1, max_length=128)


class MachineState(BaseModel):
    model_config = ConfigDict(extra="forbid")
    admin_state: Literal["enabled", "draining", "disabled"]


class IssueCredential(BaseModel):
    model_config = ConfigDict(extra="forbid")
    lifetime_seconds: int = Field(strict=True, ge=1, le=31_536_000)


async def management_runtime(request):
    if getattr(request.state, "auth_source", None) != AUTH_SOURCE_SESSION or request.headers.get("authorization") is not None or request.headers.get(INTERNAL_AUTH_HEADER_NAME) is not None:
        raise HTTPException(status_code=403, detail="Administrator session required without other credentials")
    await require_admin_user(request, detail="Fleet management requires an administrator")
    actor = await get_current_user(request)
    if actor is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    runtime = get_fleet_runtime(request.app)
    if getattr(runtime, "management", None) is None:
        raise HTTPException(status_code=503, detail="Fleet management unavailable")
    return runtime.management, actor


async def mutation(operation):
    try:
        return await operation
    except LookupError:
        raise HTTPException(status_code=404, detail="Node or credential not found") from None
    except (FileExistsError, ValueError):
        raise HTTPException(status_code=409, detail="Node management conflicts with retained state") from None


@router.post("", status_code=201)
async def register_machine(body: RegisterMachine, request: Request):
    management, actor = await management_runtime(request)
    try:
        return await management.register(operator_id=actor, **body.model_dump())
    except FileExistsError:
        raise HTTPException(status_code=409, detail="Node identity already registered") from None
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid node identity, capacity or profile allowlist") from None


@router.get("/{node_id}")
async def machine_status(node_id: str, request: Request):
    management, _ = await management_runtime(request)
    return await mutation(management.status(node_id))


@router.patch("/{node_id}")
async def machine_state(node_id: str, body: MachineState, request: Request):
    management, _ = await management_runtime(request)
    return await mutation(management.set_state(node_id, body.admin_state))


@router.delete("/{node_id}", status_code=204)
async def delete_machine(node_id: str, request: Request):
    management, _ = await management_runtime(request)
    await mutation(management.delete(node_id))
    return Response(status_code=204)


@router.post("/{node_id}/credentials", status_code=201)
async def issue_credential(node_id: str, body: IssueCredential, request: Request, response: Response):
    management, _ = await management_runtime(request)
    issued = await mutation(management.issue(node_id, body.lifetime_seconds))
    response.headers["Cache-Control"] = "no-store"
    return {"credential_id": issued.credential_id, "node_id": issued.node_id, "token": issued.token}


@router.delete("/{node_id}/credentials/{credential_id}", status_code=204)
async def revoke_credential(node_id: str, credential_id: str, request: Request):
    management, _ = await management_runtime(request)
    await mutation(management.revoke(node_id, credential_id))
    return Response(status_code=204)
