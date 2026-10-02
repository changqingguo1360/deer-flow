"""Own SCRAM database and actual TCP node route for Linux runner acceptance."""

import asyncio
import os
import secrets
import socket
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

from deerflow_ecs_fleet.worker.containers import DockerContainers
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@dataclass(repr=False)
class ControlDatabase:
    engine: object
    session_factory: object
    schema: str
    runner_url: str = field(repr=False)
    host_url: str = field(repr=False)
    password: str = field(repr=False)
    port: int


@asynccontextmanager
async def control_database(tmp_path):
    driver = DockerContainers(state_dir=tmp_path / "pg-state")
    name = "fleet-c04-pg-" + uuid.uuid4().hex
    password = secrets.token_hex(32)
    private = tmp_path / "control-password"
    private.write_text(password)
    private.chmod(0o600)
    engine = None
    try:
        await driver.checked(
            "run",
            "--detach",
            "--name",
            name,
            "--publish",
            "127.0.0.1::5432",
            "--env",
            "POSTGRES_USER=fleet_c04",
            "--env",
            "POSTGRES_PASSWORD_FILE=/run/control-password",
            "--env",
            "POSTGRES_HOST_AUTH_METHOD=scram-sha-256",
            "--mount",
            "type=bind,src=" + str(private) + ",dst=/run/control-password,readonly",
            "postgres@sha256:721873c34ceb9f8d8fc265984940dc982404c105f19ad51be9fdc5970a6080ea",
            timeout=30,
        )
        observed = await driver.inspect(name)
        port = int(observed["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"])
        host_url = "postgresql+asyncpg://fleet_c04:" + password + "@127.0.0.1:" + str(port) + "/postgres"
        runner_url = "postgresql+asyncpg://fleet_c04:" + password + "@host.docker.internal:" + str(port) + "/postgres"
        engine = create_async_engine(host_url)
        deadline = asyncio.get_running_loop().time() + 30
        while True:
            try:
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                break
            except Exception:
                if asyncio.get_running_loop().time() >= deadline:
                    raise RuntimeError("Own C04 SCRAM database did not become ready") from None
                await asyncio.sleep(0.1)
        schema = "c04_" + uuid.uuid4().hex
        async with engine.begin() as conn:
            await conn.execute(text('CREATE SCHEMA "' + schema + '"'))
        await engine.dispose()
        engine = create_async_engine(host_url, connect_args={"server_settings": {"search_path": schema}})
        yield ControlDatabase(engine, async_sessionmaker(engine, expire_on_commit=False), schema, runner_url, host_url, password, port)
    finally:
        if engine is not None:
            await engine.dispose()
        await driver.command("rm", "--force", name, timeout=30)
        private.unlink(missing_ok=True)


@asynccontextmanager
async def node_server(app):
    import uvicorn

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="off"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        deadline = asyncio.get_running_loop().time() + 10
        while not server.started:
            if task.done():
                await task
                raise RuntimeError("Actual node HTTP server stopped before readiness")
            if asyncio.get_running_loop().time() >= deadline:
                raise RuntimeError("Actual node HTTP server readiness timeout")
            await asyncio.sleep(0.01)
        yield "http://127.0.0.1:" + str(sock.getsockname()[1])
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 10)
        except BaseException:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        sock.close()


async def local_parity_run(db, private_payload, body, user, directory):
    """Run the same actual Gateway/lead/task path on this process, with PG state."""
    import copy
    import sys
    from contextlib import AsyncExitStack, ExitStack

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from app.fleet.runner_context import execution_configuration
    from app.gateway import services
    from deerflow.config.app_config import AppConfig, set_app_config
    from deerflow.config.extensions_config import extensions_config_scope
    from deerflow.config.paths import get_paths
    from deerflow.mcp.cache import mcp_tools_scope
    from deerflow.mcp.session_pool import get_session_pool
    from deerflow.mcp.tools import get_mcp_tools
    from deerflow.models.credentials import model_credential_scope
    from deerflow.persistence.agent_definition_context import agent_definition_store_scope
    from deerflow.persistence.agents.sql import SqlAgentStore
    from deerflow.persistence.managed_subagents.sql import SqlManagedSubagentStore
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime import RunManager
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.store.async_provider import make_store
    from deerflow.runtime.stream_bridge import make_stream_bridge

    from .test_c02_remote_agent_admission import request

    payload = copy.deepcopy(private_payload)
    payload["database"]["postgres_url"] = db.host_url
    payload["extensions"]["mcpServers"]["c04"]["command"] = sys.executable
    payload["extensions"]["mcpServers"]["c04"]["args"] = [str(Path(__file__).with_name("c04_mcp_fixture.py"))]
    private = AppConfig.model_validate(payload)
    public, resolver = execution_configuration(private)
    skill_root = directory / "skills"
    for name in ("enabled", "disabled"):
        target = skill_root / "public" / ("c04-" + name)
        target.mkdir(parents=True)
        (target / "SKILL.md").write_text("---\nname: c04-" + name + "\ndescription: C04 deterministic skill state acceptance\n---\nProduce deterministic artifacts.\n")
    public.skills.path = str(skill_root)
    from deerflow.config.sandbox_config import VolumeMountConfig

    public.sandbox.mounts = [VolumeMountConfig(host_path=str(Path(__file__).parent), container_path="/mnt/c04-runtime", read_only=True)]
    set_app_config(public)
    old_home = os.environ.get("DEER_FLOW_HOME")
    old_path = os.environ.get("PATH", "")
    os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + old_path
    os.environ["DEER_FLOW_HOME"] = str(directory / "home")
    try:
        async with AsyncExitStack() as resources:
            checkpointer = await resources.enter_async_context(make_checkpointer(private))
            store = await resources.enter_async_context(make_store(private))
            bridge = await resources.enter_async_context(make_stream_bridge(private))
            sync_engine = create_engine(private.database.app_sync_sqlalchemy_url, connect_args={"options": "-csearch_path=" + db.schema})
            resources.callback(sync_engine.dispose)
            sync_sf = sessionmaker(sync_engine, expire_on_commit=False)
            from deerflow_c04_fixture.memory import PostgresMemory

            from deerflow.agents.memory import manager as memory_factory

            memory_factory._memory_manager = PostgresMemory.from_config({}, mode="tool", session_factory=sync_sf, memory_table="c06_local_memory")
            resources.callback(memory_factory.reset_memory_manager)
            definitions = (SqlAgentStore(private.database.app_sync_sqlalchemy_url, session_factory=sync_sf), SqlManagedSubagentStore(private.database.app_sync_sqlalchemy_url, session_factory=sync_sf))
            from app.mcp_tasks.service import McpTaskService
            from deerflow.mcp.task_tool_caller import McpTaskToolCaller
            from deerflow.mcp.tasks import ORDINARY_MCP_TASK_DRIVER, McpTaskDriverRegistry, OrdinaryMcpTaskDriver
            from deerflow.mcp.tasks.runtime import set_mcp_task_config_snapshot, set_mcp_task_submitter
            from deerflow.persistence.mcp_tasks import McpTaskRepository

            drivers = McpTaskDriverRegistry()
            drivers.register(ORDINARY_MCP_TASK_DRIVER, OrdinaryMcpTaskDriver(McpTaskToolCaller(private.extensions)))
            submitter = McpTaskService(repository=McpTaskRepository(db.session_factory), drivers=drivers, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
            set_mcp_task_submitter(submitter)
            set_mcp_task_config_snapshot(private.extensions)
            resources.callback(set_mcp_task_submitter, None)
            resources.callback(set_mcp_task_config_snapshot, None)
            resources.push_async_callback(get_session_pool().close_all)
            with ExitStack() as scopes:
                scopes.enter_context(model_credential_scope(resolver))
                scopes.enter_context(extensions_config_scope(private.extensions))
                scopes.enter_context(agent_definition_store_scope(*definitions))
                tools = await get_mcp_tools()
                scopes.enter_context(mcp_tools_scope(tools))
                manager = RunManager(store=RunRepository(db.session_factory))
                req = request(manager, user)
                req.app.state.stream_bridge = bridge
                req.app.state.checkpointer = checkpointer
                req.app.state.store = store
                req.app.state.run_event_store = DbRunEventStore(db.session_factory)
                req.app.state.run_events_config = private.run_events
                req.app.state.thread_store = ThreadMetaRepository(db.session_factory)
                get_paths().ensure_thread_dirs("thread-c04-local", user_id=user.id)
                (get_paths().sandbox_work_dir("thread-c04-local", user_id=user.id) / "source.txt").write_bytes(b"c04-")
                (get_paths().sandbox_uploads_dir("thread-c04-local", user_id=user.id) / "source.txt").write_bytes(b"artifact\n")
                record = await services.start_run(body, "thread-c04-local", req)
                await asyncio.wait_for(record.task, 45)
                assert record.status == "success"
                local = await checkpointer.aget_tuple({"configurable": {"thread_id": "thread-c04-local"}})
                remote = await checkpointer.aget_tuple({"configurable": {"thread_id": "thread-c04"}})
                assert local is not None and remote is not None
                outputs = get_paths().sandbox_outputs_dir("thread-c04-local", user_id=user.id)
                artifacts = {name: (outputs / name).read_bytes() for name in ("parent.txt", "child.txt")}
                return local, remote, artifacts
    finally:
        os.environ["PATH"] = old_path
        if old_home is None:
            os.environ.pop("DEER_FLOW_HOME", None)
        else:
            os.environ["DEER_FLOW_HOME"] = old_home


def semantic_messages(checkpoint):
    """Keep content/tools/usage; normalize only process paths and generated ids."""
    import re

    def stable(value):
        if isinstance(value, dict):
            return {key: stable(item) for key, item in value.items() if key not in {"id", "tool_call_id"}}
        if isinstance(value, list):
            return [stable(item) for item in value]
        if isinstance(value, str):
            value = re.sub(r"<current_date>[^<]+</current_date>", "<current_date><execution-date></current_date>", value)
            value = value.replace("python /mnt/c04-runtime/c04_tool_probe.py", "python -m fleet.c04_tool_probe")
            value = re.sub(r"mcp-task-[0-9a-f]{32}", "<generated-mcp-task>", value)
            value = re.sub(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", "<generated-id>", value)
            return value
        return value

    messages = checkpoint.checkpoint["channel_values"]["messages"]
    return [stable({"type": message.type, "content": message.content, "tool_calls": getattr(message, "tool_calls", []), "usage": getattr(message, "usage_metadata", None)}) for message in messages]


async def local_parity_subprocess(db, private, body, user, directory):
    import json
    import sys

    payload = {"host_url": db.host_url, "schema": db.schema, "private": private, "body": body.model_dump(mode="json"), "user_id": user.id, "directory": str(directory)}
    runtime_root = Path(__file__).parents[2]
    paths = [
        str(Path(__file__).parents[1]),
        str(runtime_root),
        str(runtime_root / "packages/harness"),
        str(runtime_root / "packages/extension-api"),
        str(runtime_root / "packages/ecs-fleet"),
        str(Path(__file__).parent / "fixtures/c04-runtime-plugin"),
    ]
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([*paths, env.get("PYTHONPATH", "")])
    process = await asyncio.create_subprocess_exec(sys.executable, "-m", "fleet.c04_local_parity_worker", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(json.dumps(payload).encode() + b"\n"), 60)
    except BaseException:
        process.kill()
        await process.wait()
        raise
    if process.returncode:
        diagnostic = Path("/private/tmp/c04-local-parity-diagnostic.log")
        diagnostic.write_bytes(stderr.replace(db.password.encode(), b"[control credential redacted]"))
        diagnostic.chmod(0o600)
        raise AssertionError("Actual Local parity process failed; redacted diagnostic saved")
    result = json.loads(stdout.splitlines()[-1])
    summary = Path("/private/tmp/c04-parity-summary.json")
    summary.write_text(json.dumps(result))
    summary.chmod(0o600)
    return result
