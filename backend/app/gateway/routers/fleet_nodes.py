"""Worker routes deliberately separate from session-authenticated extension routes."""

from typing import Literal

from fastapi import APIRouter, HTTPException, Request
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
