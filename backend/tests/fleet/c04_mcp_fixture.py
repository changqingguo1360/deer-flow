"""Actual stdio MCP data service used by the bounded runner acceptance."""

from mcp.server.fastmcp import FastMCP

server = FastMCP("c04-fixture")


@server.tool()
def echo(value: str) -> str:
    """Return the deterministic acceptance value."""
    import os

    if os.environ.get("ERP_AUTH") != "c04-target-access":
        raise PermissionError("MCP target authentication rejected")
    return value


@server.tool()
def submit_job(value: str) -> dict[str, str]:
    """Submit a deterministic long-running external job."""
    echo(value)
    import uuid

    return {"task_id": "c04-external-job-" + value + "-" + uuid.uuid4().hex, "status": "running"}


@server.tool()
def job_status(task_id: str) -> dict[str, str]:
    """Return the external job state (host pollers only)."""
    echo(task_id)
    return {"task_id": task_id, "status": "completed", "result": "finished"}


@server.tool()
def cancel_job(task_id: str) -> dict[str, str]:
    """Cancel an external job (host cancellation workers only)."""
    echo(task_id)
    return {"task_id": task_id, "status": "cancelled"}


if __name__ == "__main__":
    server.run(transport="stdio")
