"""Finite new-user-turn policy, preserving actual original stock validation."""

import re

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage


def continuation_reply(messages):
    selected = [(index, re.search(r"c08-continuation=([a-z0-9-]+)", str(message.content))) for index, message in enumerate(messages) if isinstance(message, HumanMessage)]
    selected = [(index, match) for index, match in selected if match is not None]
    assert selected, "Current continuation user input is required"
    index, match = selected[-1]
    assert any(isinstance(message, ToolMessage) and message.name == "present_files" and message.status != "error" for message in messages[:index]), "Materialized accepted source history is required"
    marker = match.group(1)
    tools = [message for message in messages[index + 1 :] if isinstance(message, ToolMessage)]
    assert all(message.status != "error" for message in tools), "Actual continuation tool failed"
    ids = {message.tool_call_id for message in tools}
    calls = []
    if "c08-" + marker + "-write" not in ids:
        calls = [
            {
                "id": "c08-" + marker + "-write",
                "type": "tool_call",
                "name": "bash",
                "args": {"description": "Continue from immutable accepted artifacts", "command": "test -s /mnt/user-data/outputs/parent.txt && printf 'continuation-" + marker + "\\n' >> /mnt/user-data/outputs/parent.txt"},
            }
        ]
    elif "c08-" + marker + "-present" not in ids:
        calls = [{"id": "c08-" + marker + "-present", "type": "tool_call", "name": "present_files", "args": {"filepaths": ["/mnt/user-data/outputs/parent.txt", "/mnt/user-data/outputs/child.txt"]}}]
    return AIMessage(id="c08-" + marker + "-" + str(len(tools)), content="" if calls else "Continuation completed", tool_calls=calls, usage_metadata={"input_tokens": 11, "output_tokens": 7, "total_tokens": 18})


async def build_environment(*, bootstrap, spec, grant):
    """Observe actual original graph/saver; never manufacture checkpoint state."""
    from dataclasses import replace

    from app.fleet.runner_context import build_agent_environment
    from fleet.c04_worker_fixture import ScriptedModel
    from fleet.c08_linux_fixture import receipt
    from fleet.c08_stock_linux_fixture import install_final_closed_service_probe

    if bootstrap is not None:
        from fleet.c08_installed_bytes import verify_installed

        verify_installed()
    environment = await build_agent_environment(bootstrap=bootstrap, spec=spec, grant=grant)
    publisher = environment.workspace_publications
    saver = environment.context.checkpointer
    original_reply = ScriptedModel.reply
    original_callback = saver.after_root_commit
    original_prepare = environment.context.prepare_terminal
    original_close = environment.close
    from deerflow_c04_fixture import Service

    restore_probe = install_final_closed_service_probe(publisher, Service)
    calls = 0

    def observed_reply(model, messages):
        nonlocal calls
        # Keep credential, scope, skill and definition-store checks from the
        # exact original provider before selecting this new-turn fixture policy.
        original_reply(model, messages)
        assert model.model == "parent", "Continuation must not replay delegated child work"
        calls += 1
        assert calls <= 3, "Continuation finite model budget exceeded"
        result = continuation_reply(messages)
        receipt(
            "original-continuation-materialized-model-input",
            run_id=spec.run_id,
            thread_id=spec.thread_id,
            messages=[{"type": message.type, "id": message.id, "tool_call_id": getattr(message, "tool_call_id", None), "name": getattr(message, "name", None)} for message in messages],
            calls=[{"id": call["id"], "name": call["name"]} for call in result.tool_calls],
            model_call=calls,
        )
        return result

    async def observed_callback(config, metadata):
        # The saver has already exited its original transaction and stamped
        # this exact current run; read the stored tuple independently.
        current = await saver.aget_tuple(config)
        assert current is not None
        assert current.metadata["deerflow_execution_run_id"] == spec.run_id
        receipt(
            "original-continuation-current-run-checkpoint",
            run_id=spec.run_id,
            thread_id=spec.thread_id,
            checkpoint_id=current.config["configurable"]["checkpoint_id"],
            stored_run_stamp=current.metadata["deerflow_execution_run_id"],
            checkpoint_step=current.metadata.get("step"),
        )
        await original_callback(config, metadata)

    async def prepare(record):
        restore_probe.capture_owners()
        await original_prepare(record)

    async def close():
        deadline = publisher.teardown.budget._deadline
        try:
            await original_close()
            if deadline is not None:
                assert publisher.teardown.budget._deadline == deadline
            receipt("original-continuation-closed", run_id=spec.run_id, model_calls=calls, final_deadline_preserved=deadline is not None and publisher.teardown.budget._deadline == deadline)
        finally:
            restore_probe()
            ScriptedModel.reply = original_reply

    ScriptedModel.reply = observed_reply
    saver.after_root_commit = observed_callback
    environment.context = replace(environment.context, prepare_terminal=prepare)
    environment.close = close
    return environment


def compatibility():
    from app.fleet.runner_context import installed_compatibility

    return installed_compatibility()


build_environment.worker_compatibility = compatibility
