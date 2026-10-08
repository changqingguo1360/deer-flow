"""Prebuild construction and native SQL contracts for the installed observer."""

from types import SimpleNamespace

import pytest

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment
from .test_c08_terminal_pair import prepared_pair as prepared_pair


@pytest.mark.asyncio
async def test_installed_observer_preserves_frozen_stock_context(monkeypatch):
    from deerflow.runtime.runs.worker import RunContext
    from fleet.c04_worker_fixture import ScriptedModel

    from . import c08_stock_linux_fixture as fixture

    async def prepare(record):
        pass

    async def callback(config, metadata):
        pass

    async def close():
        pass

    async def accept(*args, **kwargs):
        pass

    publisher = SimpleNamespace(sf=object(), pool=object(), scope_key="original-scope", accept_partial=accept)
    original_context = RunContext(checkpointer=SimpleNamespace(after_root_commit=callback), prepare_terminal=prepare, checkpoint_channel_mode="delta", checkpoint_snapshot_frequency=3, checkpoint_durability="sync")
    environment = SimpleNamespace(context=original_context, workspace_publications=publisher, close=close)

    async def build(**kwargs):
        return environment

    monkeypatch.setattr("app.fleet.runner_context.build_agent_environment", build)
    original_reply = ScriptedModel.reply
    try:
        result = await fixture.build_environment(bootstrap=None, spec=None, grant=None)
        assert result.context is not original_context
        assert result.context.prepare_terminal is not prepare
        assert result.context.checkpoint_channel_mode == "delta"
        assert result.context.checkpoint_snapshot_frequency == 3
        assert result.context.checkpoint_durability == "sync"
        assert original_context.prepare_terminal is prepare
        assert result.context.checkpointer.after_root_commit is not callback
        await result.close()
    finally:
        ScriptedModel.reply = original_reply


@pytest.mark.asyncio
async def test_installed_observer_reads_actual_committed_request_point_join(prepared_pair, monkeypatch, capsys):
    from deerflow.persistence.run.sql import RunRepository

    from .c08_stock_linux_fixture import accepted_pair_row
    from .test_c08_terminal_pair import participant

    pair = prepared_pair
    repository = RunRepository(pair.sf, mutation_capability=pair.capability)
    repository._terminal_participant = participant(pair)
    with pair.scope():
        await repository.update_status(pair.identity.run_id, status="success")
    async with pair.sf() as session:
        row = await accepted_pair_row(session, pair.identity.request_id)
    assert tuple(row) == (pair.identity.checkpoint_id, pair.candidate.manifest_id, "accepted")
    # Exercise the actual postcommit finishing query through the observer's
    # wrapper too; production terminal mutation above already committed.
    import json

    from deerflow.runtime.runs.worker import RunContext
    from fleet.c04_worker_fixture import ScriptedModel

    from . import c08_stock_linux_fixture as fixture

    async def no_op(*args, **kwargs):
        pass

    manager = SimpleNamespace(persist_current_status=no_op, set_status_if_not_cancelled=no_op)
    publisher = SimpleNamespace(sf=pair.sf, pool=object(), scope_key="original", accept_partial=no_op, terminal=SimpleNamespace(prepared=(pair.identity, pair.candidate, 1)))
    environment = SimpleNamespace(context=RunContext(checkpointer=SimpleNamespace(after_root_commit=no_op), prepare_terminal=no_op), workspace_publications=publisher, manager=manager, close=no_op)

    async def build(**kwargs):
        return environment

    monkeypatch.setattr("app.fleet.runner_context.build_agent_environment", build)
    original_reply = ScriptedModel.reply
    try:
        observed = await fixture.build_environment(bootstrap=None, spec=pair.item.spec, grant=None)
        await observed.manager.persist_current_status(pair.identity.run_id)
        receipts = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
        committed = next(item for item in receipts if item["event"] == "original-stock-terminal-committed-finishing")
        assert committed["both_pointers_equal"] and committed["core_status"] == "success"
        assert committed["task_state"] == committed["placement_state"] == "finishing"
        await observed.close()
    finally:
        ScriptedModel.reply = original_reply


def test_selected_stock_factory_uses_original_compatibility_contract(monkeypatch):
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
    from deerflow_ecs_fleet.worker import agent_environment

    from . import c08_stock_linux_fixture as fixture

    expected = WorkerCompatibility(runtime_digest="sha256:" + "a" * 64, skill_snapshot={"entries": []}, plugin_snapshot={"entries": []}, workspace_contract_version=1)
    selections = []

    def select(provider):
        selections.append(provider)
        return fixture.build_environment

    monkeypatch.setattr(agent_environment, "installed_environment_factory", select)
    monkeypatch.setattr("app.fleet.runner_context.installed_compatibility", lambda: expected)
    assert agent_environment.worker_compatibility("c08-stock") is expected
    assert selections == ["c08-stock"]


@pytest.mark.asyncio
@pytest.mark.parametrize("pause", [False, True])
async def test_stock_provider_stream_protocol_keeps_two_presentation_turns(monkeypatch, pause):
    """Fixture-only protocol: outcomes/gates are simulated, no tools or SQL run."""
    from langchain_core.messages import HumanMessage, ToolMessage, message_chunk_to_message
    from langgraph.graph.message import add_messages
    from pydantic import SecretStr

    from deerflow.agents.middlewares.dangling_tool_call_middleware import DanglingToolCallMiddleware
    from deerflow.runtime.runs.worker import RunContext
    from fleet.c04_worker_fixture import ScriptedModel

    from . import c08_stock_linux_fixture as fixture

    async def no_op(*args, **kwargs):
        pass

    class Owner:
        def __init__(self, finished=False):
            self.finished = finished

        def done(self):
            return self.finished

    controller = SimpleNamespace(_closed=False, _final=False)
    pool = SimpleNamespace(_entries={})
    publisher = SimpleNamespace(sf=object(), pool=pool, scope_key="original", accept_partial=no_op, controller=controller)
    environment = SimpleNamespace(context=RunContext(checkpointer=SimpleNamespace(after_root_commit=no_op), prepare_terminal=no_op), workspace_publications=publisher, close=no_op)

    async def build(**kwargs):
        return environment

    from deerflow.config.app_config import AppConfig

    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "memory": {"enabled": True, "mode": "tool", "manager_class": "deerflow_c04_fixture.memory:PostgresMemory"}})
    monkeypatch.setattr("app.fleet.runner_context.build_agent_environment", build)
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: config)
    monkeypatch.setattr("deerflow.config.extensions_config.get_scoped_extensions_config", lambda: object())
    monkeypatch.setattr("deerflow.persistence.agent_definition_context.get_scoped_definition_stores", lambda: object())
    monkeypatch.setattr("deerflow.skills.get_or_new_skill_storage", lambda: SimpleNamespace(load_skills=lambda **kwargs: [SimpleNamespace(name="c04-enabled", enabled=True), SimpleNamespace(name="c04-disabled", enabled=False)]))
    original_reply = ScriptedModel.reply
    try:
        observed = await fixture.build_environment(bootstrap=None, spec=None, grant=None)
        # Simulate only the accepted boundary/owner states, never label them
        # actual publication or managed MCP lifecycle evidence.
        closure = dict(zip(ScriptedModel.reply.__code__.co_freevars, [cell.cell_contents for cell in ScriptedModel.reply.__closure__]))["observed"]
        model = ScriptedModel(api_key=SecretStr("fixture-private-value"))
        messages = [HumanMessage(content="c04-control-port=1234 " + ("c08-stock-pause" if pause else "c08-stock-normal"), id="initial")]
        stages = []
        middleware = DanglingToolCallMiddleware()
        for turn in range(14):
            model_input = middleware._build_patched_messages(messages) or messages
            chunk = next(model._stream(model_input)).message
            result = message_chunk_to_message(chunk)
            messages = add_messages(messages, [result])
            if not result.tool_calls:
                break
            call = result.tool_calls[0]
            stages.append(call["name"])
            if call["name"] == "ask_clarification":
                break
            if call["name"] == "present_files":
                closure["accepted"] += 1
                if closure["accepted"] == 1:
                    closure["owner"] = Owner(finished=True)
                    pool._entries[("c04", "original")] = (None, None, Owner())
            messages = add_messages(
                messages, [ToolMessage(content="Successfully presented files" if call["name"] == "present_files" else "original protocol outcome", name=call["name"], tool_call_id=call["id"], id="tool-turn-" + str(turn))]
            )
        assert stages == ["memory_add", "bash", "c04_echo", "c04_submit_job", "task", "present_files", "bash", "c04_echo", "present_files"] + (["ask_clarification"] if pause else [])
        assert closure["accepted"] == 2
        assert len([m for m in messages if isinstance(m, ToolMessage) and m.name == "present_files"]) == 2
        assert closure["model_calls"] == {"parent": 10}
        assert closure["tool_calls"] == {"parent": 10 if pause else 9}
        with pytest.raises(AssertionError, match="parent model round budget"):
            next(model._stream(messages))
        await observed.close()
    finally:
        ScriptedModel.reply = original_reply


def test_installed_stock_request_uses_original_ui_recursion_budget():
    import ast
    from pathlib import Path

    path = Path(__file__).with_name("test_c08_stock_linux_workspace.py")
    tree = ast.parse(path.read_text())
    call = next(node for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "RunCreateRequest")
    config = next(value.value for value in call.keywords if value.arg == "config")
    budget = next(value.value for key, value in zip(config.keys, config.values) if isinstance(key, ast.Constant) and key.value == "recursion_limit")
    assert budget == 1000, "extended finite stock sequence needs the original UI recursion budget"
