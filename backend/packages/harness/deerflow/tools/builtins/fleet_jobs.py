"""Submit detached computation using server identities and approved profiles."""

from typing import Annotated, Any

from langchain.tools import tool

from deerflow.agents.middlewares.input_sanitization_middleware import neutralize_untrusted_tags
from deerflow.mcp.tasks import TaskSubmitRequest
from deerflow.mcp.tasks.fleet_runtime import get_fleet_job_submitter
from deerflow.mcp.tasks.invocation import durable_tool_invocation_id
from deerflow.tools.types import Runtime


@tool
async def submit_fleet_job(
    runtime: Runtime,
    task_name: Annotated[str, "A short name for this background computation."],
    profile: Annotated[str, "An operator-approved job profile name."],
    argv: Annotated[list[str], "Program and arguments inside the isolated job container."],
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
            driver_data={"invocation_id": invocation},
        ),
    )
    return {"task_id": created["id"], "task_name": neutralize_untrusted_tags(task_name), "status": created["status"], "message": "Background job submitted; completion is tracked automatically."}
