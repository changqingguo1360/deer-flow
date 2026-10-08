"""Original C04 Service stop, real PG/private scopes and SDK after C08 closure.

Host native fixture contract only; installed full lifecycle remains a separate gate.
"""

import importlib.util
import sys
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from .test_c08_terminal_pair import admission as admission
from .test_c08_terminal_pair import checkpoint_owner as checkpoint_owner
from .test_c08_terminal_pair import owner_environment as owner_environment
from .test_c08_terminal_pair import prepared_pair as prepared_pair

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["final", "nonfinal", "no-C08", "wrong-context", "wrong-credentials", "unjoined-owner", "unexpected-error", "unclosed-gate"])
async def test_c08_final_closed_probe_preserves_original_service_stop_receipt(prepared_pair, monkeypatch, tmp_path, case):
    from mcp import ClientSession

    from deerflow.config.app_config import AppConfig, pop_current_app_config, push_current_app_config
    from deerflow.config.extensions_config import ExtensionsConfig, extensions_config_scope
    from deerflow.config.paths import Paths
    from deerflow.extensions.gateway import _TerminalExtensionOperations
    from deerflow.mcp import tools
    from deerflow.mcp.cache import mcp_tools_scope
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.models.credentials import model_credential_scope
    from deerflow.persistence.agent_definition_context import agent_definition_store_scope
    from deerflow.persistence.agents.sql import SqlAgentStore
    from deerflow.persistence.base import Base
    from deerflow.persistence.managed_subagents.sql import SqlManagedSubagentStore
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope
    from fleet.test_c08_terminal_pair import participant

    p = prepared_pair
    async with p.item.engine.begin() as c:
        await c.run_sync(Base.metadata.create_all)
        await c.run_sync(lambda conn: RunEventRow.__table__.create(conn, checkfirst=True))
        schema = await c.scalar(text("SELECT current_schema()"))
    engine = create_engine(p.item.private.database.app_sync_sqlalchemy_url, connect_args={"options": "-csearch_path=" + schema})
    sf = sessionmaker(engine, expire_on_commit=False)
    definitions = (
        SqlAgentStore(p.item.private.database.app_sync_sqlalchemy_url, session_factory=sf, mutation_capability=p.capability),
        SqlManagedSubagentStore(p.item.private.database.app_sync_sqlalchemy_url, session_factory=sf, mutation_capability=p.capability),
    )
    cfg = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "models": [{"name": "model-1", "use": "fleet.c04_worker_fixture:ScriptedModel", "model": "parent"}]})
    ext = ExtensionsConfig.model_validate({"mcpServers": {"c04": {"command": sys.executable, "args": [str(ROOT / "backend/tests/fleet/c04_mcp_fixture.py")], "env": {"ERP_AUTH": "c04-target-access"}}}})
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 30
    pool = MCPSessionPool()
    scope = p.capability.context.user_id + ":" + p.capability.context.thread_id
    pool.manage_scope(scope)
    monkeypatch.setattr(tools, "get_session_pool", lambda: pool)
    monkeypatch.setattr(tools, "get_paths", lambda: Paths(tmp_path / "home"))
    path = ROOT / "backend/tests/fleet/fixtures/c04-runtime-plugin/deerflow_c04_fixture/__init__.py"
    spec = importlib.util.spec_from_file_location("original_c04_service_diagnosis", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    service = module.Service()
    service._lead = None
    from fleet import c08_stock_linux_fixture as fixture

    publisher = SimpleNamespace(controller=controller, pool=pool, scope_key=scope, capability=p.capability)
    original_check = module.Service.check
    if case == "unexpected-error":

        async def faulty_check(service):
            if controller._final:
                raise ValueError("injected original check failure")
            return await original_check(service)

        module.Service.check = faulty_check
    restore = fixture.install_final_closed_service_probe(publisher, module.Service)
    if case == "no-C08":
        restore()
    calls = []
    original_call_tool = ClientSession.call_tool

    async def counted_call_tool(session, *args, **kwargs):
        calls.append(args[0] if args else kwargs.get("name"))
        return await original_call_tool(session, *args, **kwargs)

    monkeypatch.setattr(ClientSession, "call_tool", counted_call_tool)
    observations = {"source": "original unchanged C04 Service.stop; native PG/private model/fenced stores/managed stdio SDK; not installed Linux", "before_check": False}
    try:
        with p.scope(), workspace_writer_scope(controller), model_credential_scope(lambda name, use: {"api_key": "diagnosis-private-binding"}), extensions_config_scope(ext), agent_definition_store_scope(*definitions):
            push_current_app_config(cfg)
            try:
                actual = await tools.get_mcp_tools()
                with mcp_tools_scope(actual):
                    await service.check()
                    observations["before_check"] = True
                    assert calls == ["echo"]
                    if case in {"nonfinal", "no-C08"}:
                        # Original stop calls original check and the real MCP
                        # tool again, with no lead receipt before core terminal.
                        await service.stop()
                        assert not controller._closed and not controller._final
                        assert pool.scope_owners_pending(scope)
                        assert calls == ["echo", "echo"]
                        return
                    owners = [entry[2] for entry in pool._entries.values()]
                    assert len(owners) == 1
                    await controller.close_and_wait(deadline=time.monotonic() + 10, final=True)
                    pool.freeze_scope(scope, barrier_epoch=controller.barrier_epoch)
                    if case != "unjoined-owner":
                        await pool.close_scope_and_join(scope, deadline=time.monotonic() + 10)
                        assert all(owner.done() for owner in owners) and not pool.scope_owners_pending(scope)
                    repo = RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p))
                    await repo.update_status(p.identity.run_id, p.identity.desired_core_status)
                    async with p.sf() as session:
                        states = (await session.execute(text("SELECT r.status,t.state,p.state FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_agent_tasks t ON t.id=p.agent_task_id"))).one()
                    observations["states"] = list(states)
                    service._lead = SimpleNamespace(task_id=p.identity.run_id)
                    service._outcome = {"success": "completed", "interrupted": "aborted", "error": "failed", "timeout": "failed"}[p.identity.desired_core_status]
                    service._deps = SimpleNamespace(terminal_operations=_TerminalExtensionOperations(p.sf, p.capability))
                    if case == "wrong-context":
                        with remote_mutation_scope(replace(p.capability.context, attempt_id="different-original-attempt")):
                            with pytest.raises(OwnershipRejected, match="scope changed"):
                                await service.stop()
                    elif case == "wrong-credentials":
                        with model_credential_scope(lambda name, use: {}):
                            with pytest.raises(ValueError, match="private provider binding"):
                                await service.stop()
                    elif case == "unjoined-owner":
                        with pytest.raises(OwnershipRejected, match="not positively joined"):
                            await service.stop()
                        await pool.close_scope_and_join(scope, deadline=time.monotonic() + 10)
                    elif case == "unexpected-error":
                        with pytest.raises(ValueError, match="injected original check failure"):
                            await service.stop()
                    elif case == "unclosed-gate":
                        # Inject contradictory fixture read state; never use
                        # this synthetic case as a reachable production proof.
                        controller._closed = False
                        try:
                            with pytest.raises(OwnershipRejected, match="not finally settled"):
                                await service.stop()
                        finally:
                            controller._closed = True
                    else:
                        await service.stop()
                        await service.stop()  # original host receipt stays idempotent
                    assert calls == ["echo"]
                    async with p.sf() as session:
                        count = await session.scalar(text("SELECT count(*) FROM run_events WHERE event_type='run.extension.task_stop'"))
                    assert count == (1 if case == "final" else 0)
                    assert tuple(states) == (p.identity.desired_core_status, "finishing", "finishing")
                    assert not pool._entries and not pool.scope_owners_pending(scope) and controller.active_count == 0
                    observations["owners_joined"] = all(owner.done() for owner in owners)
                    observations["no_reopen"] = True
            finally:
                pop_current_app_config()
    finally:
        await pool.close_all()
        engine.dispose()
        restore()
        assert module.Service.check is (faulty_check if case == "unexpected-error" else original_check)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prepared_pair",
    [
        {"kind": "paused", "core": "interrupted", "task": "input_required", "placement": "cancelled"},
        {"kind": "final", "core": "error", "task": "failed", "placement": "failed"},
        {"kind": "final", "core": "timeout", "task": "timed_out", "placement": "timed_out"},
    ],
    indirect=True,
)
async def test_c08_closed_service_keeps_original_outcome_receipt(prepared_pair, monkeypatch, tmp_path):
    await test_c08_final_closed_probe_preserves_original_service_stop_receipt(prepared_pair, monkeypatch, tmp_path, "final")
