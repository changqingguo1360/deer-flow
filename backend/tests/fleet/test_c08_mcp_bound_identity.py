"""Installed default:default leak reproduced with the original real SDK wrapper."""

import sys
import time
from pathlib import Path

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("missing-runtime", "closed-gate", "different-context", "different-controller"))
async def test_trusted_c_factory_keeps_original_mcp_identity_and_gate_without_runtime(monkeypatch, tmp_path, case):
    from deerflow.config.extensions_config import ExtensionsConfig, extensions_config_scope
    from deerflow.config.paths import Paths
    from deerflow.mcp import tools
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.runtime.execution.mutation_context import RemoteMutationContext, remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

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
    pool = MCPSessionPool()
    scope = context.user_id + ":" + context.thread_id
    pool.manage_scope(scope)
    monkeypatch.setattr(tools, "get_session_pool", lambda: pool)
    monkeypatch.setattr(tools, "get_paths", lambda: Paths(tmp_path))
    config = ExtensionsConfig.model_validate({"mcpServers": {"c04": {"command": sys.executable, "args": [str(Path(__file__).with_name("c04_mcp_fixture.py"))], "env": {"ERP_AUTH": "c04-target-access"}}}})
    with remote_mutation_scope(context), workspace_writer_scope(controller), extensions_config_scope(config):
        actual = await tools.get_mcp_tools()
    echo = next(tool for tool in actual if tool.name == "c04_echo")
    try:
        if case in {"different-context", "different-controller"}:
            from dataclasses import replace

            from deerflow.runtime.execution.mutation_context import OwnershipRejected

            different = remote_mutation_scope(replace(context, attempt_id="different-attempt")) if case == "different-context" else workspace_writer_scope(WorkspaceWriterController())
            with different:
                with pytest.raises(OwnershipRejected):
                    await echo.ainvoke({"value": "must-not-cross-original-owners"})
            assert not pool._entries
        elif case == "closed-gate":
            await controller.close_and_wait(deadline=time.monotonic() + 2)
            with pytest.raises(RuntimeError):
                await echo.ainvoke({"value": "must-not-create-new-session"})
            assert not pool._entries
        else:
            result = await echo.ainvoke({"value": "actual-host-bound-sdk"})
            assert "actual-host-bound-sdk" in str(result)
            assert set(pool._entries) == {("c04", scope)}
            owner = pool._entries[("c04", scope)][2]
            await pool.close_scope_and_join(scope, deadline=time.monotonic() + 5)
            assert owner.done() and not pool.scope_owners_pending(scope)
            assert not pool._entries
    finally:
        await pool.close_all()


from .test_c02_remote_agent_admission import admission as admission  # noqa: E402,F401
from .test_c03_remote_agent_admission import owner_environment as owner_environment  # noqa: E402,F401
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner  # noqa: E402,F401


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("closed-gate", "missing-runtime", "spoofed-runtime"))
async def test_original_durable_submit_cannot_escape_closed_original_gate_when_controller_context_is_lost(checkpoint_owner, monkeypatch, tmp_path, case):  # noqa: F811
    from types import SimpleNamespace

    from app.fleet.mutation import FleetMutationCapability
    from app.mcp_tasks import McpTaskService
    from deerflow.config.extensions_config import ExtensionsConfig, extensions_config_scope
    from deerflow.config.paths import Paths
    from deerflow.mcp import tools
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.mcp.task_tool_caller import McpTaskToolCaller
    from deerflow.mcp.tasks import ORDINARY_MCP_TASK_DRIVER, McpTaskDriverRegistry, OrdinaryMcpTaskDriver
    from deerflow.mcp.tasks.runtime import mcp_task_submitter_scope
    from deerflow.persistence.mcp_tasks import McpTaskRepository
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope

    item = checkpoint_owner
    capability = FleetMutationCapability(item.identity, item.spec)
    context = capability.context
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 10
    pool = MCPSessionPool()
    scope = context.user_id + ":" + context.thread_id
    pool.manage_scope(scope)
    monkeypatch.setattr(tools, "get_session_pool", lambda: pool)
    monkeypatch.setattr("deerflow.mcp.task_tool_caller.get_session_pool", lambda: pool)
    monkeypatch.setattr(tools, "get_paths", lambda: Paths(tmp_path))
    monkeypatch.setattr("deerflow.mcp.task_tool_caller.get_paths", lambda: Paths(tmp_path))
    config = ExtensionsConfig.model_validate(
        {
            "mcpServers": {
                "c04": {
                    "command": sys.executable,
                    "args": [str(Path(__file__).with_name("c04_mcp_fixture.py"))],
                    "env": {"ERP_AUTH": "c04-target-access"},
                    "task_toolsets": [{"name": "c04-job", "submit_tool": "submit_job", "status_tool": "job_status", "cancel_tool": "cancel_job"}],
                }
            }
        }
    )
    from deerflow.persistence.mcp_tasks.model import McpTaskRow

    async with item.env[0].begin() as connection:
        await connection.run_sync(lambda conn: McpTaskRow.__table__.create(conn, checkfirst=True))
    repository = McpTaskRepository(item.env[1], mutation_capability=capability)
    drivers = McpTaskDriverRegistry()
    drivers.register(ORDINARY_MCP_TASK_DRIVER, OrdinaryMcpTaskDriver(McpTaskToolCaller(config)))
    from deerflow.config.mcp_tasks_config import McpTasksConfig

    defaults = McpTasksConfig()
    submitter = McpTaskService(repository=repository, drivers=drivers, poll_interval_seconds=defaults.poll_interval_seconds, lease_seconds=defaults.lease_seconds, max_concurrent_polls=defaults.max_concurrent_polls)
    with remote_mutation_scope(context), workspace_writer_scope(controller), mcp_task_submitter_scope(submitter, config), extensions_config_scope(config):
        actual = await tools.get_mcp_tools()
    submit = next(tool for tool in actual if tool.name == "c04_submit_job")
    if case == "closed-gate":
        await controller.close_and_wait(deadline=time.monotonic() + 2)
    runtime = SimpleNamespace(context={"user_id": context.user_id, "thread_id": context.thread_id, "run_id": context.run_id}, tool_call_id="original-submit-call", config={}, server_info=None)
    try:
        from sqlalchemy import text

        if case == "closed-gate":
            with remote_mutation_scope(context), mcp_task_submitter_scope(submitter, config):
                with pytest.raises(RuntimeError):
                    await submit.coroutine(runtime=runtime, value="must-not-submit-after-closed")
            assert not pool._entries
            async with item.env[0].connect() as connection:
                assert await connection.scalar(text("SELECT count(*) FROM mcp_tasks")) == 0
        else:
            if case == "missing-runtime":
                runtime = None
            else:
                runtime.context = {"user_id": "wrong-user", "thread_id": "wrong-thread", "run_id": "wrong-run"}
            created = await submit.coroutine(runtime=runtime, value="actual-original-durable-submit")
            assert created["status"] == "submitted"
            assert set(pool._entries) == {("c04", scope)}
            original_owner = pool._entries[("c04", scope)][2]
            async with item.env[0].connect() as connection:
                row = (await connection.execute(text("SELECT user_id,thread_id,run_id FROM mcp_tasks"))).one()
                assert tuple(row) == (context.user_id, context.thread_id, context.run_id)
            await pool.close_scope_and_join(scope, deadline=time.monotonic() + 5)
            assert original_owner.done() and not pool.scope_owners_pending(scope)
    finally:
        await pool.close_all()
