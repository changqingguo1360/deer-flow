"""Dependency-neutral owned API reader and bounded, allowlisted IM text."""

import re
from urllib.parse import quote

import httpx

MAX_SUMMARY_BYTES = 4096
UNAVAILABLE = "Remote Agent tracking unavailable."
STATUSES = {
    "queued": "Queued",
    "running": "Running remotely",
    "waiting_jobs": "Waiting for computation",
    "paused": "Paused",
    "input_required": "Input required",
    "unknown": "Needs confirmation",
    "finishing": "Finishing",
    "recovery_required": "Needs confirmation",
    "succeeded": "Completed",
    "failed": "Failed",
    "cancelled": "Cancelled",
    "timed_out": "Timed out",
}
JOB_STATES = {"staged", "queued", "claimed", "running", "succeeded", "failed", "cancelled", "unknown", "quarantined"}
RUN_STATES = {"pending", "running", "success", "error", "interrupted", "timeout"}
SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def identifier(value):
    return value if isinstance(value, str) and SAFE_ID.fullmatch(value) else None


def generation(value):
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def format_fleet_summary(tasks):
    if not isinstance(tasks, list):
        return UNAVAILABLE
    if not tasks:
        return ""
    lines = ["Remote Agent goals:"]

    def append(line):
        # Reserve the final marker and the manager's two-newline separator.
        # IDs are retained whole or omitted.
        if len(("\n".join(lines + [line, "Further observations omitted."])).encode("utf-8")) > MAX_SUMMARY_BYTES - 2:
            return False
        lines.append(line)
        return True

    selected = sorted((task for task in tasks[:20] if isinstance(task, dict)), key=lambda task: identifier(task.get("task_id")) or "")
    truncated = len(tasks) > 20
    for task in selected:
        task_id, goal_generation = identifier(task.get("task_id")), generation(task.get("generation"))
        state = task.get("state")
        if not task_id or goal_generation is None or state not in STATUSES:
            continue
        label = STATUSES[state]
        held = task.get("resources_held") is True
        stop = task.get("stop_state") if task.get("stop_state") in {"confirmed", "unconfirmed", "not_started"} else "unconfirmed"
        if task.get("recovery_required") is True:
            label = "Needs confirmation"
        elif held and (state in {"succeeded", "failed", "cancelled", "timed_out"} or task.get("run_status") in {"success", "error", "interrupted", "timeout"}):
            label = "Stop unconfirmed"
            stop = "unconfirmed"
        elif task.get("cancel_requested") is True and (held or stop == "unconfirmed"):
            label = "Stopping"
        if not append(f"Goal {task_id} · generation {goal_generation}: {label}"):
            truncated = True
            break
        if not append(f"STOP: {stop}; resources held: {'yes' if held else 'no'}; cancellation requested: {'yes' if task.get('cancel_requested') is True else 'no'}"):
            truncated = True
            break
        current = identifier(task.get("current_run_id"))
        run_status = task.get("run_status") if task.get("run_status") in RUN_STATES else "unknown"
        if current and not append(f"Current run: {current} ({run_status})"):
            truncated = True
            break
        remaining_links = 20
        for collection in ("jobs", "runs"):
            values = task.get(collection)
            if not isinstance(values, list):
                continue
            key = "job_id" if collection == "jobs" else "run_id"
            selected_links = sorted((value for value in values[:20] if isinstance(value, dict)), key=lambda value: identifier(value.get(key)) or "")
            included_links = 0
            for item in selected_links[:remaining_links]:
                related_id, related_generation = identifier(item.get(key)), generation(item.get("generation"))
                if not related_id or related_generation is None:
                    continue
                history = " (history)" if related_generation != goal_generation else ""
                if collection == "jobs":
                    state = item.get("state") if item.get("state") in JOB_STATES else "unknown"
                    mode = item.get("link_mode") if item.get("link_mode") in {"awaited", "detached"} else "unknown"
                    line = f"Job {related_id} · generation {related_generation}{history}: {state}, {mode}"
                    parent = identifier(item.get("parent_run_id"))
                    manifest = identifier(item.get("accepted_manifest_id"))
                    if parent:
                        line += f"; parent run {parent}"
                    if manifest:
                        line += f"; accepted result {manifest}"
                else:
                    state = item.get("run_status") if item.get("run_status") in RUN_STATES else "unknown"
                    line = f"Run {related_id} · generation {related_generation}{history}: {state}"
                if not append(line):
                    truncated = True
                    break
                remaining_links -= 1
                included_links += 1
            if len(values) > included_links or task.get(collection + "_truncated") is True:
                truncated = True
    if truncated:
        lines.append("Further observations omitted.")
    return "\n".join(lines)


async def read_fleet_summary(gateway_url, thread_id, *, headers):
    # An unresolved owner must never acquire global internal caller authority.
    if not headers or not headers.get("X-DeerFlow-Owner-User-Id"):
        return UNAVAILABLE
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{gateway_url}/api/threads/{quote(thread_id, safe='')}/agent-tasks?limit=20", headers=headers, timeout=3)
            # FastAPI's absent optional route is distinct from an owned thread
            # denial. Disabled Fleet must preserve ordinary Local/B replies.
            if response.status_code == 404 and response.json() == {"detail": "Not Found"}:
                return ""
            response.raise_for_status()
            return format_fleet_summary(response.json())
    except (httpx.HTTPError, ValueError, TypeError):
        return UNAVAILABLE
