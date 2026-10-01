"""Outbound-only node protocol client; no user/session identity is inherited."""

from urllib.parse import urlsplit

import httpx


class NodeClient:
    def __init__(self, *, gateway_url: str, credential: str, timeout_seconds: float = 10, http_client=None):
        url = urlsplit(gateway_url)
        if url.scheme not in {"http", "https"} or url.username or url.password or url.query or url.fragment:
            raise ValueError("Invalid Gateway URL")
        if url.scheme != "https" and url.hostname not in {"localhost", "127.0.0.1", "::1"} and http_client is None:
            raise ValueError("Node credentials require HTTPS outside loopback")
        if not credential.startswith("df_fleet_"):
            raise ValueError("Node credential required")
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
        return await self.call("claims", {"node_session_id": self.session_id})

    async def attempt(self, claim, operation, **fields):
        return await self.call("attempts/" + claim["attempt_id"] + "/" + operation, {"node_session_id": self.session_id, "token": claim["token"], **fields})

    async def close(self):
        if self._owned:
            await self._client.aclose()
