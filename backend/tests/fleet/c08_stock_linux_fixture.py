"""Observe original installed stock execution without substituting publication.

The original C04 lead/child sequence creates six supervised writer descendants.
Two actual root presentations surround a new bash write and managed MCP call.
The production saver callback accepts both partial pairs and production worker
prepares/pairs its final or original clarification-END pause boundary.
The C08-only service stop probe checks closed resources rather than starting
new MCP work after permanent final closure; the original stop receipt remains.
"""

import hashlib
import importlib
import re
from pathlib import Path

from fleet.c08_linux_fixture import PublicationBeginObserver, original_writer_ancestry, receipt


async def build_environment(*, bootstrap, spec, grant):
    from dataclasses import replace

    from langchain_core.messages import AIMessage, ToolMessage

    from app.fleet.runner_context import build_agent_environment
    from fleet.c04_worker_fixture import ScriptedModel

    if bootstrap is not None:
        from fleet.c08_installed_bytes import verify_installed

        verify_installed()
    environment = await build_agent_environment(bootstrap=bootstrap, spec=spec, grant=grant)
    publisher = environment.workspace_publications
    original_sf = publisher.sf
    pool, scope = publisher.pool, publisher.scope_key
    observed = {"first": False, "owner": None, "accepted": 0, "model_calls": {}, "tool_calls": {}}
    original_reply = ScriptedModel.reply
    original_callback = environment.context.checkpointer.after_root_commit
    original_accept = publisher.accept_partial
    original_prepare = environment.context.prepare_terminal
    original_manager_terminal = {name: getattr(environment.manager, name) for name in ("persist_current_status", "set_status_if_not_cancelled")} if hasattr(environment, "manager") else {}

    origins = {}
    for name in (
        "deerflow_ecs_fleet.worker.agent_runner",
        "deerflow.agents.lead_agent.agent",
        "deerflow.runtime.runs.worker",
        "deerflow.runtime.runs.manager",
        "deerflow.runtime.checkpointer.fenced_saver",
        "deerflow.persistence.run.sql",
        "deerflow.runtime.execution.workspace_boundary",
        "app.fleet.workspace",
        "deerflow.agents.middlewares.clarification_middleware",
        "fleet.c04_worker_fixture",
    ):
        path = Path(importlib.import_module(name).__file__).resolve()
        origins[name] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    receipt("original-installed-origins", modules=origins)

    def observed_reply(model, messages):
        observed["model_calls"][model.model] = observed["model_calls"].get(model.model, 0) + 1
        assert observed["model_calls"][model.model] <= (10 if model.model == "parent" else 3), model.model + " model round budget exceeded"
        receipt(
            "original-stock-model-input-identities",
            model=model.model,
            messages=[{name: getattr(message, name, None) for name in ("type", "id", "name", "tool_call_id", "status")} for message in messages],
            accepted_count=observed["accepted"],
            gate_closed=publisher.controller._closed,
        )
        result = original_reply(model, messages)  # retain original provider/skill/scope checks
        failed_bash = [(message, re.search(r"(?:^|\n)Exit Code: (-?\d+)(?:\n|$)", str(message.content))) for message in messages if isinstance(message, ToolMessage) and message.name == "bash"]
        failed_bash = [(message, int(match.group(1))) for message, match in failed_bash if match is not None and int(match.group(1)) != 0]
        if "c08-stock-failed" in str(messages) and failed_bash:
            receipt(
                "original-stock-observed-actual-tool-failure",
                model=model.model,
                failed_tool_ids=[message.tool_call_id for message, _ in failed_bash],
                actual_exit_codes=[code for _, code in failed_bash],
                original_tool_statuses=[message.status for message, _ in failed_bash],
                original_tool_contents=[str(message.content) for message, _ in failed_bash],
            )
            raise RuntimeError("Original stock observed actual failed bash tool")
        receipt("original-stock-provider-returned-calls", model=model.model, message_id=result.id, calls=[{"name": call["name"], "id": call["id"]} for call in result.tool_calls])
        for call in result.tool_calls:
            if call["name"] == "bash":
                call["args"]["command"] += " && python -m fleet.c08_linux_probe --output /mnt/user-data/outputs/" + model.model + "-extra"
                if "c08-stock-failed" in str(messages) and model.model == "parent":
                    call["args"]["command"] += " && false"
        tools = [message for message in messages if isinstance(message, ToolMessage)]
        if model.model != "parent" or result.tool_calls:
            observed["tool_calls"][model.model] = observed["tool_calls"].get(model.model, 0) + len(result.tool_calls)
            tool_limit = (10 if "c08-stock-pause" in str(messages) else 9) if model.model == "parent" else 2
            assert observed["tool_calls"][model.model] <= tool_limit, model.model + " tool round budget exceeded"
            receipt("original-stock-model-chosen-branch", model=model.model, branch="original-provider", message_id=result.id)
            return result
        assert all(message.status != "error" for message in tools), "Actual original tool execution failed"
        presentations = [message for message in tools if message.name == "present_files"]
        calls = []
        if len(presentations) == 1:
            assert observed["accepted"] == 1 and not publisher.controller._closed
            if not any(message.tool_call_id == "c08-second-write" for message in tools):
                calls = [
                    {"id": "c08-second-write", "type": "tool_call", "name": "bash", "args": {"description": "Write after the accepted partial commit", "command": "printf 'accepted-second-turn\\n' >> /mnt/user-data/outputs/parent.txt"}}
                ]
            elif not any(message.tool_call_id == "c08-second-mcp" for message in tools):
                calls = [{"id": "c08-second-mcp", "type": "tool_call", "name": "c04_echo", "args": {"value": "c08-same-owner-after-accepted-partial"}}]
            else:
                owner = pool._entries[("c04", scope)][2]
                assert observed["owner"].done() and owner is not observed["owner"] and not owner.done()
                receipt("original-mcp-reconnected-after-accepted-partial", original_owner_done=True, new_owner_distinct=True, original_scope=scope)
                calls = [{"id": "c04-parent-present", "type": "tool_call", "name": "present_files", "args": {"filepaths": ["/mnt/user-data/outputs/parent.txt", "/mnt/user-data/outputs/child.txt"]}}]
        elif len(presentations) == 2:
            assert observed["accepted"] == 2
            if "c08-stock-pause" in str(messages):
                calls = [{"id": "c08-original-clarification", "type": "tool_call", "name": "ask_clarification", "args": {"question": "Approve the two published artifacts?", "clarification_type": "missing_info"}}]
            receipt("original-stock-two-presentation-turns", tools=[message.name for message in tools], accepted_count=observed["accepted"])
        else:
            raise AssertionError("Original stock model completed without two actual presentations")
        observed["tool_calls"][model.model] = observed["tool_calls"].get(model.model, 0) + len(calls)
        assert observed["tool_calls"][model.model] <= (10 if "c08-stock-pause" in str(messages) else 9)
        receipt("original-stock-finite-protocol-counts", model_calls=dict(observed["model_calls"]), tool_calls=dict(observed["tool_calls"]))
        if calls:
            receipt(
                "original-stock-model-chosen-branch",
                model=model.model,
                branch="second-turn" if len(presentations) == 1 else "clarification",
                message_id="c08-stock-" + str(len(tools)),
                calls=[{"name": call["name"], "id": call["id"]} for call in calls],
            )
            return AIMessage(id="c08-stock-" + str(len(tools)), content="", tool_calls=calls, usage_metadata=result.usage_metadata)
        receipt("original-stock-model-chosen-branch", model=model.model, branch="normal-completion", message_id=result.id)
        return result

    async def observed_callback(config, metadata):
        receipt(
            "original-stock-root-callback-identities",
            checkpoint_id=config["configurable"].get("checkpoint_id"),
            checkpoint_ns=config["configurable"].get("checkpoint_ns", ""),
            pending_message_ids=[value[1] for value in publisher.controller.pending_presentations],
            accepted_count=observed["accepted"],
            checkpoint_step=metadata.get("step"),
        )
        if publisher.controller.pending_presentations and not observed["first"]:
            ancestry, handles = original_writer_ancestry(publisher, spec)
            observed["owner"] = pool._entries[("c04", scope)][2]
            assert not observed["owner"].done()
            publisher.sf = PublicationBeginObserver(original_sf, publisher, ancestry, handles)
            observed["first"] = True
        await original_callback(config, metadata)

    async def observed_accept(identity, candidate, *, barrier_epoch):
        from deerflow.mcp.session_pool import McpScopeBarrierClosed
        from deerflow.runtime.execution.mutation_context import OwnershipRejected

        assert publisher.controller._closed and not publisher.controller._final
        try:
            publisher.controller.reserve()
        except OwnershipRejected as error:
            assert "gate is closed" in str(error)
        else:
            raise AssertionError("Prepared-only candidate opened native writer admission")
        try:
            await pool.get_session("c04", scope, {"transport": "stdio", "command": "must-not-start"})
        except McpScopeBarrierClosed:
            pass
        else:
            raise AssertionError("Prepared-only candidate opened managed MCP admission")
        receipt("original-stock-prepared-gate-closed", request_id=identity.request_id, manifest_id=candidate.manifest_id, epoch=barrier_epoch, budget_started=publisher.teardown.budget._deadline is not None)
        await original_accept(identity, candidate, barrier_epoch=barrier_epoch)
        async with original_sf() as session:
            pair = await accepted_pair_row(session, identity.request_id)
        assert tuple(pair) == (identity.checkpoint_id, candidate.manifest_id, "accepted")
        assert not publisher.controller._closed and publisher.source_version == candidate.manifest_id
        assert publisher.teardown.budget._deadline is None
        observed["accepted"] += 1
        receipt(
            "original-stock-partial-accepted-and-reopened",
            request_id=identity.request_id,
            checkpoint_id=identity.checkpoint_id,
            manifest_id=candidate.manifest_id,
            epoch=barrier_epoch,
            accepted_count=observed["accepted"],
            budget_started=False,
        )

        if observed["accepted"] == 1 and "c08-stock-partial-barrier" in str(spec.input):
            import asyncio
            import time

            receipt("original-stock-first-partial-barrier", checkpoint_id=identity.checkpoint_id, manifest_id=candidate.manifest_id)
            deadline = time.monotonic() + 90
            while not Path("/tmp/c08-first-partial-release").exists():
                if time.monotonic() >= deadline:
                    raise AssertionError("Owned first-partial observer did not release actual runner")
                await asyncio.sleep(0.05)

    async def observed_prepare(record):
        if hasattr(restore_service_check, "capture_owners"):
            restore_service_check.capture_owners()
        await original_prepare(record)
        identity, candidate, epoch = publisher.terminal.prepared
        assert publisher.controller._closed and publisher.controller._final
        receipt(
            "original-stock-final-prepared-gate-closed",
            kind=identity.kind,
            checkpoint_id=identity.checkpoint_id,
            manifest_id=candidate.manifest_id,
            epoch=epoch,
            desired_core_status=identity.desired_core_status,
            desired_task_status=identity.desired_task_status,
            desired_placement_status=identity.desired_placement_status,
            budget_started=publisher.teardown.budget._deadline is not None,
            remaining_seconds=publisher.teardown.budget.remaining(),
        )

    async def observe_committed_terminal():
        from sqlalchemy import text

        if publisher.terminal.prepared is None or observed.get("paired"):
            return
        identity, candidate, _ = publisher.terminal.prepared
        async with original_sf() as session:
            row = (
                await session.execute(
                    text(
                        "SELECT r.status,t.state AS task_state,p.state AS placement_state,t.accepted_workspace_point_id,p.final_workspace_point_id "
                        "FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id "
                        "JOIN fleet_agent_tasks t ON t.id=p.agent_task_id WHERE r.run_id=:run"
                    ),
                    {"run": identity.run_id},
                )
            ).one()
        if row.accepted_workspace_point_id != identity.request_id:
            return
        assert row.final_workspace_point_id == identity.request_id
        assert row.status == identity.desired_core_status and row.task_state == row.placement_state == "finishing"
        observed["paired"] = True
        receipt(
            "original-stock-terminal-committed-finishing",
            kind=identity.kind,
            point_id=identity.request_id,
            checkpoint_id=identity.checkpoint_id,
            manifest_id=candidate.manifest_id,
            core_status=row.status,
            task_state=row.task_state,
            placement_state=row.placement_state,
            both_pointers_equal=True,
        )

    def terminal_wrapper(original):
        async def invoke(*args, **kwargs):
            result = await original(*args, **kwargs)
            await observe_committed_terminal()
            return result

        return invoke

    for name, original in original_manager_terminal.items():
        setattr(environment.manager, name, terminal_wrapper(original))
    ScriptedModel.reply = observed_reply
    environment.context.checkpointer.after_root_commit = observed_callback
    publisher.accept_partial = observed_accept
    environment.context = replace(environment.context, prepare_terminal=observed_prepare)
    original_close = environment.close

    def restore_service_check():
        pass

    if bootstrap is not None:
        from deerflow_c04_fixture import Service

        restore_service_check = install_final_closed_service_probe(publisher, Service)

    async def close():
        deadline = publisher.teardown.budget._deadline if hasattr(publisher, "teardown") else None
        try:
            await original_close()
            if deadline is not None:
                assert publisher.teardown.budget._deadline == deadline
                assert publisher.controller._closed and publisher.controller._final
                receipt("original-stock-final-cleanup-deadline-preserved", deadline_before=deadline, deadline_after=publisher.teardown.budget._deadline, remaining_seconds=publisher.teardown.budget.remaining())
        finally:
            restore_service_check()
            ScriptedModel.reply = original_reply
            for name, original in original_manager_terminal.items():
                setattr(environment.manager, name, original)
            publisher.sf = original_sf

    environment.close = close
    return environment


async def accepted_pair_row(session, request_id):
    from sqlalchemy import text

    return (await session.execute(text("SELECT p.checkpoint_id,p.manifest_id,r.state FROM fleet_workspace_points p JOIN fleet_workspace_requests r ON r.id=p.request_id WHERE p.request_id=:id"), {"id": request_id})).one()


def compatibility():
    from app.fleet.runner_context import installed_compatibility

    return installed_compatibility()


build_environment.worker_compatibility = compatibility


def install_final_closed_service_probe(publisher, service_class):
    """C08 fixture only: original private checks plus exact closed-gate denial.

    Leave Service.stop and its host-owned terminal receipt unchanged. Outside
    this private final closure, delegate every original check without changes.
    """
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, current_remote_mutation_context
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, current_workspace_controller

    original_check = service_class.check
    original_owners = set()

    def capture_owners():
        pool, scope = publisher.pool, publisher.scope_key
        with pool._lock:
            original_owners.update(entry[2] for key, entry in pool._entries.items() if key[1] == scope)
            original_owners.update(entry[2] for key, entry in pool._inflight.items() if key[1] == scope)

    capture_owners()

    async def check(service):
        controller, pool, scope = publisher.controller, publisher.pool, publisher.scope_key
        if not controller._final:
            result = await original_check(service)
            capture_owners()
            return result
        if current_workspace_controller() is not controller or current_remote_mutation_context() != publisher.capability.context:
            raise OwnershipRejected("C08 service closed-resource scope changed")
        if not controller._closed or controller.unsettled:
            raise OwnershipRejected("C08 service original writers are not finally settled")
        with pool._lock:
            if scope not in pool._managed_scopes or pool._scope_barriers.get(scope) != controller.barrier_epoch:
                raise OwnershipRejected("C08 service original MCP scope is not frozen")
        if not original_owners or not all(owner.done() for owner in original_owners) or pool.scope_owners_pending(scope):
            raise OwnershipRejected("C08 service original MCP owners are not positively joined")
        try:
            # Original model and definition-store reads run first. The actual
            # original MCP tool then obtains no ticket, SDK session or process.
            await original_check(service)
        except OwnershipRejected as error:
            frame = error.__traceback__
            while frame.tb_next is not None:
                frame = frame.tb_next
            if str(error) != "Workspace writer gate is closed" or frame.tb_frame.f_code is not WorkspaceWriterController.reserve.__code__ or frame.tb_frame.f_locals.get("self") is not controller:
                raise
        else:
            raise AssertionError("C08 final service check unexpectedly admitted MCP work")
        if controller.unsettled or pool.scope_owners_pending(scope):
            raise OwnershipRejected("C08 service probe created unsettled resources")
        receipt("original-stock-service-final-closed-probe", original_scope=scope, original_owners_joined=True, new_tool_admission=False)

    service_class.check = check

    def restore():
        service_class.check = original_check

    restore.capture_owners = capture_owners
    return restore
