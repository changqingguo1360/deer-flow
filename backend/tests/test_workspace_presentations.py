"""Real presentation tool captures trusted graph turn rather than reducer delta."""

from types import SimpleNamespace

from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, workspace_writer_scope
from deerflow.tools.builtins.present_file_tool import present_file_tool


def test_real_present_tool_same_path_new_turn_and_retry_identity(tmp_path):
    controller = WorkspaceWriterController()
    runtime = SimpleNamespace(
        state={"thread_data": {"outputs_path": str(tmp_path)}},
        context={"user_id": "user", "run_id": "run", "thread_id": "thread"},
        tool_call_id="provider-reused",
        execution_info=SimpleNamespace(thread_id="thread", checkpoint_id="root-1", task_id="task-1", checkpoint_ns="tools:task-1"),
    )
    with workspace_writer_scope(controller):
        first = present_file_tool.func(runtime, [str(tmp_path / "same.txt")], tool_call_id="provider-reused")
        retry = present_file_tool.func(runtime, [str(tmp_path / "same.txt")], tool_call_id="provider-reused")
        runtime.execution_info = SimpleNamespace(thread_id="thread", checkpoint_id="root-2", task_id="task-2", checkpoint_ns="tools:task-2")
        new = present_file_tool.func(runtime, [str(tmp_path / "same.txt")], tool_call_id="provider-reused")
    assert first.update["messages"][0].id is not None, "remote presentation has no trusted checkpoint message identity"
    assert first.update["messages"][0].id == retry.update["messages"][0].id
    assert first.update["messages"][0].id != new.update["messages"][0].id
    assert first.update["artifacts"] == new.update["artifacts"]
    assert len(controller.pending_presentations) == 2


def test_local_presentation_retains_original_message_shape(tmp_path):
    runtime = SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}}, context={"thread_id": "thread"})
    result = present_file_tool.func(runtime, [str(tmp_path / "same.txt")], tool_call_id="local")
    assert result.update["messages"][0].id is None


def test_nested_tool_namespace_does_not_create_root_presentation(tmp_path):
    runtime = SimpleNamespace(
        state={"thread_data": {"outputs_path": str(tmp_path)}},
        context={"user_id": "user", "run_id": "run", "thread_id": "thread"},
        tool_call_id="provider",
        execution_info=SimpleNamespace(thread_id="thread", checkpoint_id="nested-root", task_id="task-1", checkpoint_ns="delegate:parent-task|tools:task-1"),
    )
    controller = WorkspaceWriterController()
    with workspace_writer_scope(controller):
        result = present_file_tool.func(runtime, [str(tmp_path / "same.txt")], tool_call_id="provider")
    assert result.update["messages"][0].id is None
    assert controller.pending_presentations == ()


def test_private_presentation_owner_cannot_be_changed(tmp_path):
    import pytest

    from deerflow.runtime.execution.mutation_context import RemoteMutationContext, remote_mutation_scope

    identity = RemoteMutationContext(
        user_id="user", thread_id="thread", run_id="run", agent_task_id="task", generation=1, node_id="node", node_session_id="session", attempt_id="attempt", owner_worker_id="owner", token_stamp="stamp", launch_spec_digest="digest"
    )
    controller = WorkspaceWriterController()
    bind = getattr(controller, "bind_execution_context", None)
    assert bind is not None, "presentation controller has no private owner binding"
    bind(identity)
    for field in ("user_id", "run_id", "thread_id"):
        runtime = SimpleNamespace(
            state={"thread_data": {"outputs_path": str(tmp_path)}},
            context={"user_id": "user", "run_id": "run", "thread_id": "thread"},
            tool_call_id="provider",
            execution_info=SimpleNamespace(thread_id="thread", checkpoint_id="root", task_id="tool-task", checkpoint_ns="tools:tool-task"),
        )
        runtime.context[field] = "other"
        with remote_mutation_scope(identity), workspace_writer_scope(controller), pytest.raises(RuntimeError, match="owner"):
            present_file_tool.func(runtime, [str(tmp_path / "same.txt")], tool_call_id="provider")
    assert controller.pending_presentations == ()
