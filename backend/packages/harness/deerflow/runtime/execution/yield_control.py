"""Host-injected cooperative END after the complete original tool step."""

from contextlib import contextmanager
from contextvars import ContextVar

from langchain.agents.middleware import AgentMiddleware, hook_config

from .mutation_context import OwnershipRejected

_yield = ContextVar("original_cooperative_yield", default=None)


class CooperativeYield:
    def __init__(self, host):
        self.host = host
        self.requested = False
        self.group = None

    async def request(self, job_ids=None):
        self.host.check_context()
        if job_ids is not None:
            await self.host.validate_requested(job_ids)
        self.requested = True

    async def prepare(self, messages):
        self.host.check_context()
        calls = {call["id"] for message in messages for call in getattr(message, "tool_calls", [])}
        paired = {message.tool_call_id for message in messages if getattr(message, "type", None) == "tool"}
        if calls - paired:
            raise OwnershipRejected("Cooperative yield requires settled original tool calls")
        self.group = await self.host.prepare()
        return self.group is not None


@contextmanager
def yield_scope(controller):
    controller.host.check_context()
    token = _yield.set(controller)
    try:
        yield
    finally:
        _yield.reset(token)


def current_yield():
    controller = _yield.get()
    if controller is not None:
        controller.host.check_context()
    return controller


class YieldMiddleware(AgentMiddleware):
    @hook_config(can_jump_to=["end"])
    async def abefore_model(self, state, runtime):
        controller = current_yield()
        if controller is not None and is_root_runtime(runtime) and controller.requested and await controller.prepare(state["messages"]):
            return {"jump_to": "end"}
        return None


def is_root_runtime(runtime):
    from langgraph._internal._constants import NS_END, NS_SEP

    context = getattr(runtime, "context", None) or {}
    info = getattr(runtime, "execution_info", None)
    if context.get("is_subagent") or info is None:
        return False
    suffix = NS_END + info.task_id
    namespace = info.checkpoint_ns
    if not isinstance(namespace, str) or not namespace.endswith(suffix):
        return False
    node = namespace[: -len(suffix)]
    return bool(node) and NS_SEP not in node and NS_END not in node
