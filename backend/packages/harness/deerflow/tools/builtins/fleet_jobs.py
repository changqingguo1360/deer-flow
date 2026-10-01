"""Submit detached computation using server identities and approved profiles."""

from typing import Annotated, Any

from langchain.tools import tool

from deerflow.agents.middlewares.input_sanitization_middleware import neutralize_untrusted_tags
from deerflow.mcp.tasks import TaskSubmitRequest
from deerflow.mcp.tasks.fleet_runtime import get_fleet_job_submitter, get_fleet_scheduled_job_slots
from deerflow.mcp.tasks.invocation import durable_tool_invocation_id
from deerflow.tools.types import Runtime


@tool
async def submit_fleet_job(
    runtime: Runtime,
    task_name: Annotated[str, "A short name for this background computation."],
    profile: Annotated[str, "An operator-approved job profile name."],
    argv: Annotated[list[str], "Program and arguments inside the isolated job container."],
    job_slot: Annotated[str | None, "Operator-approved named scheduled job slot; required for scheduled runs."] = None,
    input_manifests: Annotated[list[str] | None, "Immutable input or accepted result version IDs owned by this chat."] = None,
    code_artifact_id: Annotated[str | None, "Optional immutable code version ID; mounted read-only under /inputs/<id>."] = None,
    execution_timeout_seconds: Annotated[int, "Execution limit within the profile's authorized budget."] = 1800,
    queue_timeout_seconds: Annotated[int, "Maximum durable queue wait within the operator budget."] = 1800,
) -> dict[str, Any]:
    """Submit a durable detached Fleet job and return a background task ID.

    The Gateway tracks completion after this Agent run ends. Only declared input
    versions are mounted read-only; write results to /output. Use the chat's
    background-task tools to view status or request cancellation.
    """
    invocation = durable_tool_invocation_id(runtime)
    context = runtime.context
    thread_id = runtime.execution_info.thread_id
    if context.get("thread_id") not in {None, thread_id}:
        raise ValueError("Fleet submission requires consistent durable thread identity")
    schedule_id = context.get("scheduled_task_id")
    driver_data = {"invocation_id": invocation}
    if schedule_id is not None or job_slot is not None:
        if not isinstance(schedule_id, str) or not schedule_id:
            raise ValueError("Job slot requires an authenticated scheduled run")
        if context.get("scheduled_context_mode", "reuse_thread") != "reuse_thread":
            raise ValueError("Fleet scheduled job slots require reuse_thread")
        approved = get_fleet_scheduled_job_slots().get(job_slot) if isinstance(job_slot, str) else None
        if approved is None or approved != profile:
            raise ValueError("Scheduled job requires an approved slot and matching profile")
        driver_data.update(scheduled_task_id=schedule_id, job_slot=job_slot)
    created = await get_fleet_job_submitter().submit(
        driver_name="fleet",
        request=TaskSubmitRequest(
            user_id=context["user_id"],
            thread_id=thread_id,
            run_id=context["run_id"],
            tool_call_id=runtime.tool_call_id,
            server_name="fleet",
            task_name=task_name,
            arguments={
                "task_name": task_name,
                "profile": profile,
                "argv": argv,
                "input_manifests": input_manifests or [],
                "code_artifact_id": code_artifact_id,
                "execution_timeout_seconds": execution_timeout_seconds,
                "queue_timeout_seconds": queue_timeout_seconds,
                "link_mode": "detached",
            },
            driver_data=driver_data,
        ),
    )
    return {
        "task_id": created["id"],
        "task_name": neutralize_untrusted_tags(created["task_name"]),
        "status": created["status"],
        "message": ("Existing scheduled submission reused; original job and tracking are preserved." if created.get("reused_existing") else "Background job submitted; completion is tracked automatically."),
    }
