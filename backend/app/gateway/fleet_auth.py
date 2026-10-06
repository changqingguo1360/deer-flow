"""Host-only node authentication; optional Fleet code is never imported here."""

import re
from dataclasses import dataclass

from fastapi import HTTPException, Request

from app.gateway.internal_auth import INTERNAL_AUTH_HEADER_NAME

_NODE_PATH = re.compile(r"^/api/fleet/node/(?:session|heartbeat|claims|attempts/[A-Za-z0-9_.-]+/(?:start|renew|complete|stopped|reconcile-stopped|workspace/(?:poll|claim|prepared)))/?$")


@dataclass(frozen=True)
class FleetNodePrincipal:
    node_id: str
    credential_id: str


def is_node_route(method: str, path: str) -> bool:
    return method == "POST" and _NODE_PATH.fullmatch(path) is not None


def is_node_credential(authorization: str | None) -> bool:
    if authorization is None:
        return False
    scheme, separator, token = authorization.partition(" ")
    return bool(separator and scheme.lower() == "bearer" and token.startswith("df_fleet_"))


def get_fleet_runtime(app):
    extensions = getattr(app.state, "extensions", None)
    runtimes = [service for _, service in getattr(extensions, "services", ()) if getattr(service, "fleet_protocol_version", None) == 1]
    if len(runtimes) != 1 or not getattr(runtimes[0], "ready", False):
        raise HTTPException(status_code=503, detail="Fleet runtime unavailable")
    return runtimes[0]


async def authenticate_node(request: Request) -> FleetNodePrincipal:
    authorization = request.headers.get("authorization")
    if request.cookies.get("access_token") or request.headers.get(INTERNAL_AUTH_HEADER_NAME) or not is_node_credential(authorization):
        raise HTTPException(status_code=401, detail="Node bearer credential required without session credentials")
    runtime = get_fleet_runtime(request.app)
    try:
        identity = await runtime.credentials.authenticate(authorization)
    except PermissionError:
        raise HTTPException(status_code=401, detail="Invalid node credential") from None
    return FleetNodePrincipal(**identity)


def require_node(request: Request) -> FleetNodePrincipal:
    principal = getattr(request.state, "fleet_node", None)
    if not isinstance(principal, FleetNodePrincipal):
        raise HTTPException(status_code=401, detail="Node authentication required")
    return principal
