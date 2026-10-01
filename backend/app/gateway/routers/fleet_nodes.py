"""Worker routes deliberately separate from session-authenticated extension routes."""

from dataclasses import asdict
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from app.gateway.fleet_auth import get_fleet_runtime, require_node

router = APIRouter(prefix="/api/fleet/node", tags=["fleet-node"])


class SessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    protocol_version: Literal[1] = 1


class HeartbeatRequest(SessionRequest):
    node_session_id: str = Field(min_length=1, max_length=64)


@router.post("/session")
async def open_session(request: Request, body: SessionRequest):
    principal = require_node(request)
    runtime = get_fleet_runtime(request.app)
    try:
        return await runtime.nodes.open_session(principal.node_id, protocol_version=body.protocol_version)
    except PermissionError:
        raise HTTPException(status_code=403, detail="Node unavailable") from None


@router.post("/heartbeat")
async def heartbeat(request: Request, body: HeartbeatRequest):
    principal = require_node(request)
    runtime = get_fleet_runtime(request.app)
    try:
        return await runtime.nodes.heartbeat(principal.node_id, **body.model_dump())
    except PermissionError:
        raise HTTPException(status_code=403, detail="Node unavailable") from None
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None


class NodeSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    node_session_id: str = Field(min_length=1, max_length=64)


class AttemptRequest(NodeSessionRequest):
    token: str = Field(min_length=1, max_length=256)


class RenewRequest(AttemptRequest):
    running: bool = Field(default=False, strict=True)


class StopRequest(AttemptRequest):
    reason: Literal["exit", "cancelled", "lease_lost", "execution_deadline"]
    exit_code: int = Field(ge=0, le=255, strict=True)


@router.post("/claims")
async def claim(request: Request, body: NodeSessionRequest):
    principal = require_node(request)
    runtime = get_fleet_runtime(request.app)
    try:
        claim = await runtime.scheduler.claim_job(principal.node_id, node_session_id=body.node_session_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None
    return Response(status_code=204) if claim is None else {"kind": "job", **asdict(claim)}


async def attempt_operation(request, attempt_id, body, method):
    principal = require_node(request)
    runtime = get_fleet_runtime(request.app)
    try:
        return await getattr(runtime.attempts, method)(node_id=principal.node_id, attempt_id=attempt_id, **body.model_dump())
    except PermissionError:
        raise HTTPException(status_code=403, detail="Attempt unavailable") from None
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None


@router.post("/attempts/{attempt_id}/start")
async def start_attempt(request: Request, attempt_id: str, body: AttemptRequest):
    return await attempt_operation(request, attempt_id, body, "authorize_start")


@router.post("/attempts/{attempt_id}/renew")
async def renew_attempt(request: Request, attempt_id: str, body: RenewRequest):
    return await attempt_operation(request, attempt_id, body, "renew")


@router.post("/attempts/{attempt_id}/stopped")
async def stopped_attempt(request: Request, attempt_id: str, body: StopRequest):
    return await attempt_operation(request, attempt_id, body, "stopped")


class CompleteRequest(AttemptRequest):
    manifest: dict


@router.post("/attempts/{attempt_id}/complete")
async def complete_attempt(request: Request, attempt_id: str, body: CompleteRequest):
    principal = require_node(request)
    runtime = get_fleet_runtime(request.app)
    if runtime.manifests is None:
        raise HTTPException(status_code=503, detail="Fleet artifacts unavailable")
    try:
        return await runtime.manifests.complete(node_id=principal.node_id, attempt_id=attempt_id, **body.model_dump())
    except PermissionError:
        raise HTTPException(status_code=403, detail="Attempt unavailable") from None
    except (ValueError, OSError):
        raise HTTPException(status_code=409, detail="Completion not accepted") from None
