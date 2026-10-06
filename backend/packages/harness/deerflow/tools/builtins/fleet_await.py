"""Await this original remote Agent's owned Fleet dependencies."""

from langchain.tools import tool

from deerflow.runtime.execution.mutation_context import OwnershipRejected
from deerflow.runtime.execution.yield_control import current_yield, is_root_runtime
from deerflow.tools.types import Runtime


@tool
async def await_fleet_jobs(runtime: Runtime, job_ids: list[str] | None = None) -> str:
    """Yield this remote Agent until all its awaited Fleet jobs settle.

    Optional job_ids are the public background task IDs returned as task_id
    by submit_fleet_job; they are validated as original owned awaited tasks.
    All remaining owned awaited jobs are included. Parallel tools finish before
    the original checkpoint and files are sealed; resumption follows physical STOP.
    """
    controller = current_yield()
    if controller is None or not is_root_runtime(runtime):
        raise OwnershipRejected("Fleet await requires original bound remote execution")
    await controller.request(job_ids)
    return "Await requested. This tool step will settle before the Agent yields its owned dependencies."
