"""Outbound-only node protocol client; no user/session identity is inherited."""

from urllib.parse import urlsplit

import httpx


class NodeClient:
    def __init__(self, *, gateway_url: str, credential: str, timeout_seconds: float = 10, http_client=None, claim_kind="job", compatibility=None, compatibility_loader=None):
        url = urlsplit(gateway_url)
        if url.scheme not in {"http", "https"} or url.username or url.password or url.query or url.fragment:
            raise ValueError("Invalid Gateway URL")
        if url.scheme != "https" and url.hostname not in {"localhost", "127.0.0.1", "::1"} and http_client is None:
            raise ValueError("Node credentials require HTTPS outside loopback")
        if not credential.startswith("df_fleet_"):
            raise ValueError("Node credential required")
        if (
            claim_kind not in {"job", "agent"}
            or (claim_kind == "job" and (compatibility is not None or compatibility_loader is not None))
            or (claim_kind == "agent" and compatibility is None and not callable(compatibility_loader))
            or (compatibility_loader is not None and (compatibility is not None or not callable(compatibility_loader)))
        ):
            raise ValueError("Invalid worker claim capability")
        self.claim_kind = claim_kind
        self.compatibility = compatibility
        self._compatibility_loader = compatibility_loader
        self._credential = credential
        self._client = http_client or httpx.AsyncClient(base_url=gateway_url.rstrip("/") + "/", timeout=timeout_seconds, follow_redirects=False)
        self._owned = http_client is None
        self.node_id = None
        self.session_id = None

    async def call(self, path, body):
        response = await self._client.post("api/fleet/node/" + path, headers={"Authorization": "Bearer " + self._credential}, json=body)
        response.raise_for_status()
        return None if response.status_code == 204 else response.json()

    async def open_session(self):
        result = await self.call("session", {"protocol_version": 1})
        self.node_id = result["node_id"]
        self.session_id = result["node_session_id"]
        return result

    async def heartbeat(self):
        return await self.call("heartbeat", {"node_session_id": self.session_id, "protocol_version": 1})

    async def claim(self):
        body = {"node_session_id": self.session_id}
        if self.claim_kind == "agent":
            if self._compatibility_loader is not None:
                from ..launch_spec import WorkerCompatibility

                candidate = WorkerCompatibility.model_validate(await self._compatibility_loader())
                if candidate.workspace_contract_version != 1:
                    raise ValueError("Installed workspace contracts required for new Agent claims")
                self.compatibility = candidate.model_dump(mode="json")
                self._compatibility_loader = None
            body.update(kind="agent", compatibility=self.compatibility)
        return await self.call("claims", body)

    async def attempt(self, claim, operation, **fields):
        return await self.call("attempts/" + claim["attempt_id"] + "/" + operation, {"node_session_id": self.session_id, "token": claim["token"], **fields})

    async def reconcile_stopped(self, claim, *, original_node_session_id, **fields):
        return await self.call("attempts/" + claim["attempt_id"] + "/reconcile-stopped", {"node_session_id": self.session_id, "original_node_session_id": original_node_session_id, "token": claim["token"], **fields})

    async def close(self):
        if self._owned:
            await self._client.aclose()
