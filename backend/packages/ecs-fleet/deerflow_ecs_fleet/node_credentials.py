"""High-entropy node credentials are scoped identities, never user sessions."""

import hashlib
import hmac
import secrets
from dataclasses import dataclass, field
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import func, select

from .persistence.models import CredentialRow, NodeRow

TOKEN_PREFIX = "df_fleet_"


@dataclass(frozen=True)
class IssuedCredential:
    credential_id: str
    node_id: str
    token: str = field(repr=False)


class NodeCredentials:
    def __init__(self, session_factory):
        self.sf = session_factory

    async def issue(self, node_id: str, *, lifetime_seconds: int) -> IssuedCredential:
        if type(lifetime_seconds) is not int or not 1 <= lifetime_seconds <= 31_536_000:
            raise ValueError("Credential lifetime must be positive")
        token = TOKEN_PREFIX + secrets.token_urlsafe(48)
        credential_id = str(uuid4())
        async with self.sf.begin() as session:
            if await session.get(NodeRow, node_id, with_for_update=True) is None:
                raise ValueError("Node not found")
            now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
            session.add(CredentialRow(id=credential_id, node_id=node_id, token_hash=hashlib.sha256(token.encode()).hexdigest(), expires_at=now + timedelta(seconds=lifetime_seconds)))
        return IssuedCredential(credential_id=credential_id, node_id=node_id, token=token)

    async def revoke(self, credential_id: str, *, node_id: str | None = None) -> None:
        async with self.sf.begin() as session:
            row = await session.get(CredentialRow, credential_id, with_for_update=True)
            if node_id is not None and (row is None or row.node_id != node_id):
                raise LookupError("Credential not found for node")
            if row is not None:
                row.revoked_at = (await session.execute(select(func.clock_timestamp()))).scalar_one()

    async def authenticate(self, authorization: str) -> dict[str, str]:
        scheme, separator, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not separator or not token.startswith(TOKEN_PREFIX) or len(token) > 256:
            raise PermissionError("Invalid node credential")
        digest = hashlib.sha256(token.encode()).hexdigest()
        async with self.sf() as session:
            row = (await session.execute(select(CredentialRow).where(CredentialRow.token_hash == digest, CredentialRow.revoked_at.is_(None), CredentialRow.expires_at > func.clock_timestamp()))).scalar_one_or_none()
            if row is None or not hmac.compare_digest(row.token_hash, digest):
                raise PermissionError("Invalid node credential")
            return {"node_id": row.node_id, "credential_id": row.id}
