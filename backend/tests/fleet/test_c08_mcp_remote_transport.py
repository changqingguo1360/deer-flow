"""Actual loopback HTTP/SSE SDK transports keep the original C gate/owner."""

import asyncio
import sys
import time
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace

import pytest

from .c04_integration_fixture import node_server


@asynccontextmanager
async def actual_service(server, transport):
    """The actual server owns its AnyIO contexts independently of the caller."""
    from sse_starlette.sse import AppStatus

    # Each sequential real uvicorn instance has its own shutdown lifecycle.
    # sse-starlette retains this process-wide flag after the prior server exits.
    AppStatus.should_exit = False
    ready = asyncio.get_running_loop().create_future()
    stop = asyncio.Event()

    async def serve():
        try:
            async with AsyncExitStack() as resources:
                app = server.streamable_http_app() if transport == "http" else server.sse_app()
                if transport == "http":
                    await resources.enter_async_context(server.session_manager.run())
                url = await resources.enter_async_context(node_server(app))
                ready.set_result(url)
                await stop.wait()
        except BaseException as exc:
            if not ready.done():
                ready.set_exception(exc)
            raise

    owner = asyncio.create_task(serve())
    try:
        yield await asyncio.shield(ready)
    finally:
        stop.set()
        await asyncio.shield(owner)


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ("http", "sse"))
@pytest.mark.parametrize("case", ("closed-gate", "foreign-owner", "cancel-cleanup", "missing-context", "local"))
async def test_original_remote_mcp_sdk_preserves_bound_gate_owner_until_cleanup(monkeypatch, transport, case):
    import langchain_mcp_adapters.tools as sdk_tools
    from mcp.server.fastmcp import FastMCP

    from deerflow.config.extensions_config import ExtensionsConfig, extensions_config_scope
    from deerflow.mcp.tools import get_mcp_tools
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, RemoteMutationContext, remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    called, server_release, cleanup_entered, cleanup_release, physically_cleaned = asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []
    server = FastMCP("actual-c08-remote", stateless_http=True)

    @server.tool()
    async def echo(value: str) -> str:
        calls.append(value)
        called.set()
        if value == "actual-parked-call":
            await server_release.wait()
        return value

    context = RemoteMutationContext(
        user_id="original-user",
        thread_id="original-thread",
        run_id="original-run",
        agent_task_id="original-task",
        generation=1,
        node_id="original-node",
        node_session_id="original-session",
        attempt_id="original-attempt",
        owner_worker_id="fleet-agent:original-attempt",
        token_stamp="a" * 64,
        launch_spec_digest="sha256:" + "b" * 64,
    )
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 10
    tool_task = None
    async with actual_service(server, transport) as url:
        config = ExtensionsConfig.model_validate({"mcpServers": {"actual": {"type": transport, "session_init_timeout": 3, "url": url + ("/mcp" if transport == "http" else "/sse")}}})
        if case == "local":
            with extensions_config_scope(config):
                tools = await get_mcp_tools()
        else:
            with remote_mutation_scope(context), workspace_writer_scope(controller), extensions_config_scope(config):
                tools = await get_mcp_tools()
        actual = next(tool for tool in tools if tool.name == "actual_echo")
        original_create_session = sdk_tools.create_session

        @asynccontextmanager
        async def actual_parked_sdk_cleanup(*args, **kwargs):
            cm = original_create_session(*args, **kwargs)
            session = await cm.__aenter__()
            try:
                yield session
            except BaseException:
                exception = sys.exc_info()
                cleanup_entered.set()
                await cleanup_release.wait()
                suppressed = await cm.__aexit__(*exception)
                physically_cleaned.set()
                if not suppressed:
                    raise
            else:
                cleanup_entered.set()
                await cleanup_release.wait()
                await cm.__aexit__(None, None, None)
                physically_cleaned.set()

        try:
            if case == "closed-gate":
                await controller.close_and_wait(deadline=time.monotonic() + 2)
                with pytest.raises(RuntimeError):
                    await actual.ainvoke({"value": "must-not-call-closed"})
                assert calls == []
            elif case == "foreign-owner":
                with remote_mutation_scope(replace(context, attempt_id="another-original-attempt")):
                    with pytest.raises(OwnershipRejected):
                        await actual.ainvoke({"value": "must-not-call-foreign"})
                assert calls == []
            elif case in ("missing-context", "local"):
                result = await actual.ainvoke({"value": "actual-positive-call"})
                assert "actual-positive-call" in str(result)
                assert calls == ["actual-positive-call"] and controller.active_count == 0
            else:
                monkeypatch.setattr(sdk_tools, "create_session", actual_parked_sdk_cleanup)
                tool_task = asyncio.create_task(actual.ainvoke({"value": "actual-parked-call"}))
                await asyncio.wait_for(called.wait(), 3)
                tool_task.cancel()
                await asyncio.wait_for(cleanup_entered.wait(), 3)
                assert not physically_cleaned.is_set() and not tool_task.done()
                assert controller.active_count == 1
                with pytest.raises(TimeoutError):
                    await controller.close_and_wait(deadline=time.monotonic() + 0.03)
                assert controller.active_count == 1 and not physically_cleaned.is_set()
                cleanup_release.set()
                server_release.set()
                with pytest.raises(asyncio.CancelledError):
                    await tool_task
                assert physically_cleaned.is_set() and controller.active_count == 0
                await controller.close_and_wait(deadline=time.monotonic() + 2)
        finally:
            cleanup_release.set()
            server_release.set()
            if tool_task is not None:
                await asyncio.gather(tool_task, return_exceptions=True)
