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


if __name__ == "__main__":
    server.run(transport="stdio")
