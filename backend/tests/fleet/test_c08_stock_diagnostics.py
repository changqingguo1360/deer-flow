"""Read-only diagnostic contracts; HTTP mock is not installed Node proof."""

import json

import httpx
import pytest

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment
from .test_c08_terminal_pair import prepared_pair as prepared_pair


@pytest.mark.asyncio
async def test_original_client_observer_records_only_whitelisted_response_fields(tmp_path):
    from deerflow_ecs_fleet.worker.client import NodeClient

    from .c08_stock_diagnostics import NodeProtocolObservation

    responses = [httpx.Response(201, json={"stop": True, "reason": "cancel_requested", "token": "private-result"}), httpx.Response(409, json={"detail": "private-error-detail"})]
    transport = httpx.MockTransport(lambda request: responses.pop(0))
    async with httpx.AsyncClient(base_url="http://localhost/", transport=transport) as http:
        client = NodeClient(gateway_url="http://localhost", credential="df_fleet_private-credential", http_client=http)
        observer = NodeProtocolObservation(client, tmp_path)
        result = await client.call("attempts/private-attempt/renew", {"token": "private-body"})
        assert result["token"] == "private-result"  # delegate result is unchanged
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await client.call("attempts/private-attempt/workspace/requests", {"token": "private-body"})
        assert caught.value.response.status_code == 409
    raw = (tmp_path / "node-http-observations.json").read_text()
    assert all(value not in raw for value in ("private-credential", "private-attempt", "private-body", "private-result", "private-error-detail", "Authorization"))
    rows = json.loads(raw)
    assert [r["status_code"] for r in rows if r["event"] == "http_response"] == [201, 409]
    assert rows[1]["stop_reason"] == "cancel_requested" and rows[1]["stop"]
    assert rows[-1]["error_type"] == "HTTPStatusError"
    assert rows == observer.rows


@pytest.mark.asyncio
async def test_diagnostic_write_failure_preserves_original_http_error(tmp_path, monkeypatch):
    from pathlib import Path

    from deerflow_ecs_fleet.worker.client import NodeClient

    from .c08_stock_diagnostics import NodeProtocolObservation

    def fail_write(*args, **kwargs):
        raise OSError("fixture evidence unavailable")

    monkeypatch.setattr(Path, "write_text", fail_write)
    transport = httpx.MockTransport(lambda request: httpx.Response(403))
    async with httpx.AsyncClient(base_url="http://localhost/", transport=transport) as http:
        client = NodeClient(gateway_url="http://localhost", credential="df_fleet_fixture", http_client=http)
        NodeProtocolObservation(client, tmp_path)
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await client.call("heartbeat", {})
        assert caught.value.response.status_code == 403


@pytest.mark.asyncio
async def test_precleanup_actual_pg_select_is_whitelisted_and_does_not_mutate(prepared_pair, tmp_path):
    from .c08_stock_diagnostics import capture_precleanup, read_control_states
    from .test_c08_terminal_pair import snapshot

    pair = prepared_pair
    before = await snapshot(pair)
    rows = await read_control_states(pair.sf, pair.identity.run_id)
    assert await snapshot(pair) == before
    assert len(rows) == 1
    assert set(rows[0]) == {
        "postgres_observed_at",
        "core_status",
        "core_lease_expires_at",
        "task_state",
        "task_deadline",
        "placement_state",
        "attempt_state",
        "accepted_workspace_point_id",
        "final_workspace_point_id",
        "attempt_lease_expires_at",
        "attempt_execution_deadline",
    }

    class Driver:
        async def inspect(self, ref):
            return {"Id": "actual-observed-container", "Image": "actual-observed-image", "State": {"Running": False, "ExitCode": 137}, "Config": {"User": "123:456", "Env": ["SECRET=private-environment"]}}

    await capture_precleanup(session_factory=pair.sf, run_id=pair.identity.run_id, driver=Driver(), refs=["original-owned-ref"], evidence=tmp_path)
    assert await snapshot(pair) == before
    raw = (tmp_path / "precleanup-observations.json").read_text()
    assert "private-environment" not in raw and "Env" not in raw and "token" not in raw
    assert json.loads(raw)["docker"][0]["State"]["ExitCode"] == 137


@pytest.mark.asyncio
async def test_precleanup_diagnostic_faults_do_not_interrupt_original_resource_cleanup(tmp_path):
    from .c08_stock_diagnostics import capture_precleanup

    def unavailable_session():
        raise RuntimeError("private-database-error")

    class Driver:
        async def inspect(self, ref):
            raise RuntimeError("private-container-error")

    cleaned = False
    with pytest.raises(ValueError, match="original lifecycle failure"):
        try:
            raise ValueError("original lifecycle failure")
        finally:
            await capture_precleanup(session_factory=unavailable_session, run_id="original-run", driver=Driver(), refs=["owned"], evidence=tmp_path)
            cleaned = True
    saved = json.loads((tmp_path / "precleanup-observations.json").read_text())
    assert saved["sql_error_type"] == "RuntimeError" and saved["docker_error_types"] == ["RuntimeError"]
    assert cleaned
