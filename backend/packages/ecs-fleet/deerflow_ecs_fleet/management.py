"""Trusted management operations; the host owns session/admin/CSRF checks."""

from sqlalchemy.exc import IntegrityError


class FleetManagement:
    def __init__(self, runtime):
        self.runtime = runtime

    async def register(self, *, operator_id, profile_allowlist, **fields):
        configured = {name for name, profile in self.runtime.config.profiles.items() if profile.kind == "job"}
        if not profile_allowlist or len(profile_allowlist) != len(set(profile_allowlist)) or not set(profile_allowlist) <= configured:
            raise ValueError("Explicit configured job profiles required")
        try:
            await self.runtime.nodes.register(**fields, profile_allowlist=profile_allowlist, registered_by=operator_id)
        except IntegrityError:
            raise FileExistsError("Node identity already registered") from None
        return await self.status(fields["node_id"])

    async def status(self, node_id):
        try:
            return await self.runtime.nodes.status(node_id)
        except ValueError:
            raise LookupError("Node not found") from None

    async def set_state(self, node_id, state):
        await self.status(node_id)
        await self.runtime.nodes.set_admin_state(node_id, state)
        return await self.status(node_id)

    async def delete(self, node_id):
        await self.status(node_id)
        try:
            await self.runtime.nodes.delete(node_id)
        except IntegrityError:
            raise ValueError("Node has retained history") from None

    async def issue(self, node_id, lifetime_seconds):
        await self.status(node_id)
        return await self.runtime.credentials.issue(node_id, lifetime_seconds=lifetime_seconds)

    async def revoke(self, node_id, credential_id):
        await self.status(node_id)
        await self.runtime.credentials.revoke(credential_id, node_id=node_id)
