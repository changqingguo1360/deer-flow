"""Model-facing Fleet submission uses persisted graph identity and host task rows."""

import importlib
import importlib.util
from types import SimpleNamespace

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode
from sqlalchemy import text

from .test_b02_fleet_foundation import service_class, settings
from .test_b05_fleet_durable_jobs import setup_tracking


def modules():
    assert importlib.util.find_spec("deerflow.tools.builtins.fleet_jobs") is not None, "Controlled Fleet submission tool missing"
    assert importlib.util.find_spec("deerflow.mcp.tasks.fleet_runtime") is not None, "Fleet task runtime bridge missing"
    return importlib.import_module("deerflow.tools.builtins.fleet_jobs"), importlib.import_module("deerflow.mcp.tasks.fleet_runtime")


def test_model_schema_has_no_execution_identity_or_privileged_profile_fields():
    module, bridge = modules()
    fields = module.submit_fleet_job.tool_call_schema.model_json_schema()["properties"]
    assert {"task_name", "profile", "argv", "input_manifests", "code_artifact_id"} <= set(fields)
    assert not ({"runtime", "user_id", "thread_id", "run_id", "node_id", "token", "image", "env", "network", "link_mode", "idempotency_key"} & set(fields))
    bridge.set_fleet_job_submitter(None)
    assert not bridge.is_fleet_job_runtime_available()
    with pytest.raises(RuntimeError):
        bridge.get_fleet_job_submitter()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_tool_graph_replay_reuses_durable_job_and_tracking(fleet_database, tmp_path):
    module, bridge = modules()
    from app.mcp_tasks.service import McpTaskService
    from deerflow.mcp.tasks import McpTaskDriverRegistry
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    registry = McpTaskDriverRegistry()
    registry.register("fleet", fleet.bind_tracking(await setup_tracking(engine, sf)))
    service = McpTaskService(repository=McpTaskRepository(sf), drivers=registry, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)

    class CrashAfterCommittedTracking:
        crash = True

        async def submit(self, **kwargs):
            result = await service.submit(**kwargs)
            if self.crash:
                raise RuntimeError("Crash after durable submission")
            return result

    submitter = CrashAfterCommittedTracking()
    bridge.set_fleet_job_submitter(submitter)
    builder = StateGraph(MessagesState, context_schema=dict)
    builder.add_node("tools", ToolNode([module.submit_fleet_job], handle_tool_errors=False))
    builder.add_edge(START, "tools")
    builder.add_edge("tools", END)
    saver = InMemorySaver()
    graph = builder.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "t"}}
    context = {"user_id": "u", "run_id": "r", "thread_id": "t"}
    call = {"name": "submit_fleet_job", "args": {"task_name": "batch", "profile": "batch", "argv": ["true"]}, "id": "reused-provider-call", "type": "tool_call"}
    try:
        with pytest.raises(RuntimeError, match="Crash"):
            await graph.ainvoke({"messages": [AIMessage(content="", tool_calls=[call])]}, config, context=context)
        submitter.crash = False
        result = await builder.compile(checkpointer=saver).ainvoke(None, config, context=context)
        assert "token" not in result["messages"][-1].content and "output_prefix" not in result["messages"][-1].content
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM mcp_tasks"))).scalar_one() == 1
            job = (await conn.execute(text("SELECT user_id,thread_id,source_run_id FROM fleet_jobs"))).one()
            assert (job.user_id, job.thread_id, job.source_run_id) == ("u", "t", "r")
        await graph.ainvoke({"messages": [AIMessage(content="", tool_calls=[call])]}, config, context=context)
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_jobs"))).scalar_one() == 2
            assert (await conn.execute(text("SELECT count(*) FROM mcp_tasks"))).scalar_one() == 2
            assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
    finally:
        bridge.set_fleet_job_submitter(None)
        await fleet.stop()


@pytest.mark.asyncio
async def test_tool_rejects_missing_durable_context_before_submitting():
    module, bridge = modules()

    class UnexpectedSubmit:
        async def submit(self, **kwargs):
            raise AssertionError("Identity-less submission reached service")

    bridge.set_fleet_job_submitter(UnexpectedSubmit())
    try:
        with pytest.raises(ValueError, match="durable"):
            await module.submit_fleet_job.coroutine(runtime=SimpleNamespace(context={}, execution_info=None, tool_call_id="call"), task_name="batch", profile="batch", argv=["true"])
    finally:
        bridge.set_fleet_job_submitter(None)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_host_readiness_and_tools_visibility(fleet_database, tmp_path):
    module, bridge = modules()
    assert importlib.util.find_spec("app.fleet.runtime") is not None, "Host Fleet tool runtime binding missing"
    host = importlib.import_module("app.fleet.runtime")
    from deerflow.config.app_config import AppConfig
    from deerflow.tools.tools import get_available_tools

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    app = SimpleNamespace(state=SimpleNamespace(extensions=SimpleNamespace(services=(("fleet", fleet),))))
    try:
        for enabled, repository in [(False, True), (True, False)]:
            with pytest.raises(RuntimeError):
                host.validate_fleet_task_runtime(app, enabled=enabled, repository_available=repository)
        host.validate_fleet_task_runtime(app, enabled=True, repository_available=True)
        bridge.set_fleet_job_submitter(None)
        config = AppConfig(sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"})
        for subagent_enabled in (False, True):
            assert "submit_fleet_job" not in {tool.name for tool in get_available_tools(include_mcp=False, app_config=config, subagent_enabled=subagent_enabled)}
        with pytest.raises(RuntimeError):
            host.install_fleet_tools(app, object())
        fleet.bind_tracking(await setup_tracking(engine, sf))
        sentinel = object()
        host.install_fleet_tools(app, sentinel)
        assert bridge.get_fleet_job_submitter() is sentinel
        fleet_tool = next(tool for tool in get_available_tools(include_mcp=False, app_config=config) if tool.name == "submit_fleet_job")
        assert "Available job profiles: batch" in fleet_tool.description
        assert fleet.config.profiles["batch"].image not in fleet_tool.description
        for subagent_enabled in (False, True):
            names = [tool.name for tool in get_available_tools(include_mcp=False, app_config=config, subagent_enabled=subagent_enabled)]
            assert names.count("submit_fleet_job") == 1
        fleet.config = fleet.config.model_copy(update={"jobs_enabled": False})
        host.install_fleet_tools(app, sentinel)
        assert not bridge.is_fleet_job_runtime_available()
        for subagent_enabled in (False, True):
            assert "submit_fleet_job" not in {tool.name for tool in get_available_tools(include_mcp=False, app_config=config, subagent_enabled=subagent_enabled)}
    finally:
        bridge.set_fleet_job_submitter(None)
        await fleet.stop()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_disabled_new_jobs_keeps_existing_fleet_driver(fleet_database, tmp_path):
    from app.fleet.job_tracking import register_fleet_driver
    from deerflow.mcp.tasks import McpTaskDriverRegistry

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path).model_copy(update={"jobs_enabled": False}))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    await setup_tracking(engine, sf)
    app = SimpleNamespace(state=SimpleNamespace(extensions=SimpleNamespace(services=(("fleet", fleet),))))
    registry = McpTaskDriverRegistry()
    try:
        register_fleet_driver(app, registry)
        assert registry.get("fleet") is not None and fleet.jobs is not None
    finally:
        await fleet.stop()
