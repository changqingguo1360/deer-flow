"""Read-only host fixture diagnostics; never alter protocol or cleanup outcomes."""

import json
import time
from pathlib import Path

import httpx
from sqlalchemy import text

_STOP_REASONS = frozenset({"exit", "cancel_requested", "execution_deadline", "cancelled", "lease_lost", "node_restart"})
_OPERATIONS = frozenset({"session", "heartbeat", "claims", "attempts", "start", "renew", "stopped", "workspace", "requests", "claim", "prepared", "settled"})


def safe_operation(path):
    return "/".join(part for part in path.split("/") if part in _OPERATIONS)


def _save(path, value):
    try:
        Path(path).write_text(json.dumps(value, indent=2, default=lambda item: item.isoformat()))
    except Exception:
        # Auxiliary evidence cannot replace the original failure or cleanup.
        pass


class NodeProtocolObservation:
    def __init__(self, client, evidence):
        self.rows = []
        self.path = Path(evidence) / "node-http-observations.json"
        original_call = client.call

        async def response_hook(response):
            self.rows.append({"event": "http_response", "operation": safe_operation(response.request.url.path), "status_code": response.status_code, "host_monotonic": time.monotonic()})
            _save(self.path, self.rows)

        async def call(path, body):
            row = {"event": "original_client_call", "operation": safe_operation(path), "host_monotonic_started": time.monotonic()}
            try:
                result = await original_call(path, body)
            except BaseException as error:
                row["error_type"] = type(error).__name__
                if isinstance(error, httpx.HTTPStatusError):
                    row["status_code"] = error.response.status_code
                raise
            else:
                if isinstance(result, dict):
                    reason = result.get("reason", result.get("stop_reason"))
                    row["stop_reason"] = reason if isinstance(reason, str) and reason in _STOP_REASONS else None
                    row["stop"] = result.get("stop") if isinstance(result.get("stop"), bool) else None
                return result
            finally:
                row["host_monotonic_finished"] = time.monotonic()
                self.rows.append(row)
                _save(self.path, self.rows)

        client._client.event_hooks["response"].append(response_hook)
        client.call = call


async def read_control_states(session_factory, run_id):
    # One ordinary SELECT, no FOR UPDATE and no original publication callback
    # transaction. Explicit columns exclude launch_spec/outcome/token/contents.
    async with session_factory() as session:
        rows = await session.execute(
            text(
                "SELECT clock_timestamp() AS postgres_observed_at, r.status AS core_status, r.lease_expires_at AS core_lease_expires_at, "
                "t.state AS task_state, t.deadline AS task_deadline, p.state AS placement_state, a.state AS attempt_state, "
                "t.accepted_workspace_point_id, p.final_workspace_point_id, a.lease_expires_at AS attempt_lease_expires_at, "
                "a.execution_deadline AS attempt_execution_deadline FROM runs r "
                "JOIN fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_agent_tasks t ON t.id=p.agent_task_id "
                "LEFT JOIN fleet_attempts a ON a.id=p.active_attempt_id WHERE r.run_id=:run"
            ),
            {"run": run_id},
        )
        return [dict(row) for row in rows.mappings()]


async def capture_precleanup(*, session_factory, run_id, driver, refs, evidence):
    observed = {"scope": "original owned fixture observations before cleanup; no lifecycle substitution", "host_monotonic": time.monotonic(), "docker": []}
    try:
        observed["control_states"] = await read_control_states(session_factory, run_id)
    except BaseException as error:
        observed["sql_error_type"] = type(error).__name__
    for ref in refs:
        try:
            value = await driver.inspect(ref)
            if value is not None:
                observed["docker"].append({"Id": value["Id"], "Image": value.get("Image"), "State": value["State"], "Config": {"User": value.get("Config", {}).get("User")}})
        except BaseException as error:
            observed.setdefault("docker_error_types", []).append(type(error).__name__)
    _save(Path(evidence) / "precleanup-observations.json", observed)
