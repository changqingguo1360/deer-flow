"""Unknown execution notifications use the user tracking ID, never a Fleet handle."""

import json

import pytest
from deerflow_ecs_fleet.mcp_driver import FleetTaskDriver

from app.gateway.routers.mcp_tasks import _detail
from deerflow.persistence.mcp_tasks.model import McpTaskRow
from deerflow.persistence.mcp_tasks.sql import _notification_event


@pytest.mark.parametrize("state", ["unknown", "quarantined"])
def test_uncertain_job_handle_is_absent_from_public_status_and_notification(state):
    remote_handle = "fleet-private-execution-handle"
    snapshot = FleetTaskDriver.snapshot({"state": state, "id": remote_handle})
    row = McpTaskRow(id="user-tracking-id", task_name="batch", status=snapshot.status.value, input_required=snapshot.input_required)
    event = _notification_event(row, tracking_degraded=False)
    detail = _detail(row.to_dict(), threshold=3)
    assert event["task_id"] == detail["task_id"] == "user-tracking-id"
    for public_payload in [event, detail]:
        assert remote_handle not in json.dumps(public_payload, default=str)
        assert public_payload["input_required"]["reason"] == "execution_unknown"
        assert "operator reconciliation" in public_payload["input_required"]["message"]
