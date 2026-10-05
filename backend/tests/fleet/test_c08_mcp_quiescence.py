"""Positive completion of original MCP owner tasks, independent of Node census."""

import asyncio
import sys
import time
from pathlib import Path

import pytest

from deerflow.mcp.session_pool import MCPSessionPool


@pytest.mark.asyncio
async def test_scope_join_timeout_retains_real_owner_and_retry_joins_it():
    pool = MCPSessionPool()
    close, exiting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def actual_owner():
        await close.wait()
        exiting.set()
        await release.wait()

    task = asyncio.create_task(actual_owner())
    pool._entries[("original-server", "original-scope")] = (object(), asyncio.get_running_loop(), task, close)
    try:
        with pytest.raises(TimeoutError):
            await pool.close_scope_and_join("original-scope", deadline=time.monotonic() + 0.02)
        assert exiting.is_set() and not task.done()
        assert not pool._entries
        assert pool._scope_closing["original-scope"]
        release.set()
        await pool.close_scope_and_join("original-scope", deadline=time.monotonic() + 2)
        assert task.done() and "original-scope" not in pool._scope_closing
    finally:
        release.set()
        close.set()
        await task


@pytest.mark.asyncio
async def test_scope_join_cancellation_retains_original_owner_until_positive_retry():
    pool = MCPSessionPool()
    close, exiting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def actual_owner():
        await close.wait()
        exiting.set()
        await release.wait()

    task = asyncio.create_task(actual_owner())
    pool._entries[("server", "scope")] = (object(), asyncio.get_running_loop(), task, close)
    joining = asyncio.create_task(pool.close_scope_and_join("scope", deadline=time.monotonic() + 3))
    try:
        await exiting.wait()
        joining.cancel()
        with pytest.raises(asyncio.CancelledError):
            await joining
        assert not task.done() and pool._scope_closing["scope"]
        release.set()
        await pool.close_scope_and_join("scope", deadline=time.monotonic() + 2)
        assert task.done()
    finally:
        release.set()
        close.set()
        await task


@pytest.mark.asyncio
async def test_original_stateless_stdio_mcp_calls_close_and_reconnect_real_pool():
    pool = MCPSessionPool()
    connection = {"transport": "stdio", "command": sys.executable, "args": [str(Path(__file__).with_name("c04_mcp_fixture.py"))], "env": {"ERP_AUTH": "c04-target-access"}}
    try:
        session = await pool.get_session("c04-real", "c08-owned", connection)
        first = await session.call_tool("echo", {"value": "actual-first"})
        original_owner = pool._entries[("c04-real", "c08-owned")][2]
        assert first.content[0].text == "actual-first"
        await pool.close_scope_and_join("c08-owned", deadline=time.monotonic() + 5)
        assert original_owner.done()
        new_session = await pool.get_session("c04-real", "c08-owned", connection)
        assert new_session is not session
        second = await new_session.call_tool("echo", {"value": "actual-reconnected"})
        assert second.content[0].text == "actual-reconnected"
        await pool.close_scope_and_join("c08-owned", deadline=time.monotonic() + 5)
    finally:
        await pool.close_all()


@pytest.mark.asyncio
async def test_concurrent_scope_close_and_retry_join_same_physical_owner():
    pool = MCPSessionPool()
    close, release = asyncio.Event(), asyncio.Event()

    async def owner():
        await close.wait()
        await release.wait()

    task = asyncio.create_task(owner())
    pool._entries[("server", "scope")] = (object(), asyncio.get_running_loop(), task, close)
    first = asyncio.create_task(pool.close_scope_and_join("scope", deadline=time.monotonic() + 2))
    await close.wait()
    second = asyncio.create_task(pool.close_scope_and_join("scope", deadline=time.monotonic() + 2))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)
    assert task.done() and "scope" not in pool._scope_closing


@pytest.mark.asyncio
async def test_closed_pool_scope_rejects_new_session_until_same_epoch_reopen():
    pool = MCPSessionPool()
    pool.freeze_scope("owned", barrier_epoch=3)
    with pytest.raises(RuntimeError, match="barrier"):
        await pool.get_session("server", "owned", {"transport": "stdio", "command": "must-not-launch"})
    await pool.close_scope_and_join("owned", deadline=time.monotonic() + 1)
    with pytest.raises(ValueError, match="epoch"):
        pool.reopen_scope("owned", barrier_epoch=2)
    pool.reopen_scope("owned", barrier_epoch=3)
    assert "owned" not in pool._scope_barriers


@pytest.mark.asyncio
async def test_original_task_caller_cancel_retains_native_directory_writer(monkeypatch, tmp_path):
    import threading

    from deerflow.config.extensions_config import ExtensionsConfig
    from deerflow.mcp import task_tool_caller as module
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    controller = WorkspaceWriterController()
    config = ExtensionsConfig.model_validate({"mcpServers": {"original": {"type": "stdio", "command": "unreached"}}})
    caller = module.McpTaskToolCaller(config)

    def actual_native_prepare(connection, **kwargs):
        entered.set()
        release.wait(3)
        try:
            (tmp_path / "native-directory-proof").mkdir()
            return connection
        finally:
            finished.set()

    monkeypatch.setattr(module, "_prepare_stdio_connection", actual_native_prepare)
    with workspace_writer_scope(controller):
        task = asyncio.create_task(caller.call_tool(server_name="original", tool_name="echo", arguments={}, user_id="user", thread_id="thread"))
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not finished.is_set()
            assert controller.active_count == 1
            with pytest.raises(TimeoutError):
                await controller.close_and_wait(deadline=time.monotonic() + 0.01)
        finally:
            release.set()
            assert await asyncio.to_thread(finished.wait, 1)
        await controller.close_and_wait(deadline=time.monotonic() + 1)
    assert (tmp_path / "native-directory-proof").is_dir()
