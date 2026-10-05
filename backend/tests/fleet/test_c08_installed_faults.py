"""Transport fixture contracts on real TCP; not installed ownership acceptance."""

import httpx
import pytest
from fastapi import FastAPI


@pytest.mark.asyncio
async def test_lost_prepared_reply_reads_real_tcp_commit_then_retries(tmp_path):
    from .c04_integration_fixture import node_server
    from .c08_installed_faults import LostPreparedReply

    app = FastAPI()
    responses = []

    @app.post("/api/fleet/node/attempts/owned/workspace/prepared")
    async def prepared():
        responses.append("original-candidate")
        return {"candidate": responses[-1]}

    transport = LostPreparedReply(tmp_path)
    async with node_server(app) as url:
        async with httpx.AsyncClient(base_url=url, transport=transport) as client:
            with pytest.raises(httpx.ReadError, match="Actual committed prepared response dropped"):
                await client.post("/api/fleet/node/attempts/owned/workspace/prepared")
            retry = await client.post("/api/fleet/node/attempts/owned/workspace/prepared")
    assert retry.status_code == 200 and retry.json() == {"candidate": "original-candidate"}
    assert responses == ["original-candidate", "original-candidate"]
    assert len(transport.observations) == 1 and transport.observations[0]["actual_status"] == 200
