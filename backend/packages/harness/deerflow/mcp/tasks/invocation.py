"""Derive a durable side-effect key from the graph's persisted execution task."""

import hashlib
import json


def durable_tool_invocation_id(runtime) -> str:
    """Model/provider call IDs alone never identify an execution globally.

    LangGraph ExecutionInfo is injected by the graph, and checkpoint/task IDs
    survive replay of the same pending tool node. A new tool turn has a new
    checkpoint/task identity even when the provider reuses a tool_call_id.
    """
    info = getattr(runtime, "execution_info", None)
    context = getattr(runtime, "context", None) or {}
    if info is None:
        raise ValueError("Fleet submission requires durable graph execution context")
    parts = [context.get("user_id"), context.get("run_id"), info.thread_id, info.checkpoint_id, info.task_id, getattr(runtime, "tool_call_id", None)]
    if any(not isinstance(value, str) or not value for value in parts):
        raise ValueError("Fleet submission requires durable owner/run/checkpoint/task/tool identity")
    parts.append(info.checkpoint_ns)
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
