"""Optional host-owned workspace writer settlement, independent of Fleet/app.

Tickets describe actual work, never the lifetime of its cancellable awaiter.
An unbound Local execution keeps its original behavior.
"""

import asyncio
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

from .mutation_context import OwnershipRejected

_controller = ContextVar("workspace_writer_controller", default=None)
_ticket = ContextVar("workspace_native_writer_ticket", default=None)
_tool_invocation = ContextVar("workspace_original_tool_invocation", default=None)


class _WriterTicket:
    def __init__(self, controller):
        self.controller = controller
        self.finished = False

    @contextmanager
    def scope(self):
        token = _ticket.set(self)
        try:
            yield
        finally:
            _ticket.reset(token)

    def finish(self):
        with self.controller._condition:
            if not self.finished:
                self.finished = True
                self.controller._tickets.remove(self)
                self.controller._condition.notify_all()


class WorkspaceWriterController:
    """Thread-safe admission and positive completion for one original owner."""

    def __init__(self):
        self.process_registry = None
        self.pids_limit = None
        self.execution_deadline = None
        self._process_handles = []
        self._process_stop_deadline = None
        self._process_join_lock = threading.Lock()
        self._join_tasks = set()
        self._condition = threading.Condition()
        self._tickets = set()
        self._native_tasks = set()
        self._closed = False
        self._final = False
        self._epoch = 0
        self._execution_context = None
        self._human_inputs = {}
        self._presentations = {}
        self._accepted_presentations = set()

    def bind_execution_context(self, context):
        from .mutation_context import RemoteMutationContext

        if not isinstance(context, RemoteMutationContext):
            raise OwnershipRejected("Original presentation owner context required")
        with self._condition:
            if self._execution_context is not None and self._execution_context != context:
                raise OwnershipRejected("Original presentation owner binding changed")
            self._execution_context = context

    @property
    def pending_presentations(self):
        with self._condition:
            return tuple(value for key, value in self._presentations.items() if key not in self._accepted_presentations)

    def _root_invocation(self, runtime):
        from deerflow.mcp.tasks.invocation import durable_tool_invocation_id

        from .mutation_context import current_remote_mutation_context

        info = runtime.execution_info
        bound = self._execution_context
        current = current_remote_mutation_context()
        if current is not None or bound is not None:
            context = getattr(runtime, "context", None) or {}
            if bound is None or current != bound or any(context.get(name) != getattr(bound, name) for name in ("user_id", "thread_id", "run_id")) or info.thread_id != bound.thread_id:
                raise OwnershipRejected("Original presentation owner does not match private execution")
        key = durable_tool_invocation_id(runtime)
        # ExecutionInfo names the executing task, not the persisted saver
        # namespace. Installed Pregel appends node:task_id even for a root
        # task; nested graph parents introduce the NS_SEP separator.
        from langgraph._internal._constants import NS_END, NS_SEP

        task_ns = info.checkpoint_ns
        suffix = NS_END + info.task_id
        if not isinstance(task_ns, str) or not task_ns.endswith(suffix):
            raise OwnershipRejected("Original presentation task namespace is inconsistent")
        node_ns = task_ns[: -len(suffix)]
        if NS_SEP in node_ns:
            return None
        if not node_ns or NS_END in node_ns:
            raise OwnershipRejected("Original presentation root namespace is inconsistent")
        return key

    def observe_presentation(self, runtime, paths):
        key = self._root_invocation(runtime)
        if key is None:
            return None
        info = runtime.execution_info
        value = (key, "workspace-present-" + key, info.checkpoint_id, tuple(sorted(set(paths))))
        with self._condition:
            if self._closed:
                raise OwnershipRejected("Workspace presentation gate is closed")
            previous = self._presentations.get(key)
            if previous is not None and previous != value:
                raise OwnershipRejected("Original presentation retry conflicts")
            self._presentations[key] = value
        return value[1]

    def observe_human_input(self, runtime, request_id):
        key = self._root_invocation(runtime)
        if key is None:
            return
        if not isinstance(request_id, str) or not request_id:
            raise OwnershipRejected("Original human input request identity is missing")
        with self._condition:
            if self._closed:
                raise OwnershipRejected("Workspace human input gate is closed")
            # Retain the existing request/card protocol IDs. The private graph
            # task key distinguishes this execution's actual observation.
            self._human_inputs[request_id] = (key, runtime.tool_call_id)

    def awaiting_human_input(self, snapshot):
        from langchain_core.messages import AIMessage, ToolMessage

        messages = snapshot.values.get("messages", [])
        last_ai = next((message for message in reversed(messages) if isinstance(message, AIMessage)), None)
        if last_ai is None:
            return False
        calls = {call["id"] for call in last_ai.tool_calls if call["name"] == "ask_clarification"}
        with self._condition:
            observations = dict(self._human_inputs)
        for message in messages:
            if not isinstance(message, ToolMessage) or message.name != "ask_clarification" or message.status != "success":
                continue
            observed = observations.get(message.id)
            payload = (message.artifact or {}).get("human_input") if isinstance(message.artifact, dict) else None
            if (
                observed is not None
                and observed[1] == message.tool_call_id
                and message.tool_call_id in calls
                and isinstance(payload, dict)
                and payload.get("request_id") == message.id
                and payload.get("kind") == "human_input_request"
                and payload.get("source") == "ask_clarification"
            ):
                return True
        return False

    def accepted_presentation(self, key):
        with self._condition:
            if key not in self._presentations:
                raise OwnershipRejected("Original presentation identity is missing")
            self._accepted_presentations.add(key)

    @property
    def active_count(self):
        with self._condition:
            return len(self._tickets)

    @property
    def unsettled(self):
        with self._condition:
            return bool(self._tickets or self._process_handles or self._join_tasks)

    @property
    def barrier_epoch(self):
        with self._condition:
            return self._epoch

    def reserve(self):
        with self._condition:
            if self._closed:
                raise OwnershipRejected("Workspace writer gate is closed")
            ticket = _WriterTicket(self)
            self._tickets.add(ticket)
            return ticket

    def _wait(self, deadline):
        with self._condition:
            while self._tickets:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Actual workspace writers have not completed")
                self._condition.wait(remaining)

    def close_admission(self, *, final=False):
        """Close new writers without waiting for the existing native bodies."""
        with self._condition:
            if not self._closed:
                self._epoch += 1
            self._closed = True
            self._final = self._final or final
        return self.barrier_epoch

    async def close_and_wait(self, *, deadline, final=False):
        # Preserve normal/partial settlement ordering. Only the original
        # cancellation host requests process stop before ticket completion.
        self.close_admission(final=final)
        await asyncio.to_thread(self._wait, deadline)
        if final:
            await self._run_process_join(deadline, True)
        return self.barrier_epoch

    def bind_process_registry(self, registry, *, pids_limit):
        if self.process_registry is not None or type(pids_limit) is not int or not 1 <= pids_limit <= 4096:
            raise OwnershipRejected("Workspace process registry binding rejected")
        self.process_registry = registry
        self.pids_limit = pids_limit

    def retain_process(self, handle):
        with self._condition:
            if not any(current is handle for current in self._process_handles):
                self._process_handles.append(handle)
            deadline = self._process_stop_deadline
        if deadline is not None:
            # A launch already holding a ticket can reach retention after the
            # private host closes admission. Keep ownership and stop it too.
            handle.request_stop(deadline)

    def request_process_stop(self, *, deadline):
        """Signal retained supervisors; receipt readers and tickets stay owned."""
        with self._condition:
            if not self._closed or not self._final:
                raise OwnershipRejected("Original final gate must close before cancellation stop")
            self._process_stop_deadline = min(deadline, self._process_stop_deadline) if self._process_stop_deadline is not None else deadline
            deadline = self._process_stop_deadline
            handles = tuple(self._process_handles)
        for handle in handles:
            handle.request_stop(deadline)

    async def stop_and_join_processes(self, *, deadline):
        # The private original host owns these handles and OS pipes. A partial
        # execution deadline never reads the final cumulative cleanup budget.
        with self._condition:
            if not self._closed:
                raise OwnershipRejected("Original workspace gate must close before process stop")
        await self._run_process_join(deadline, True)

    async def join_processes(self, *, deadline):
        # Partial caller supplies the original execution deadline; no cleanup
        # budget property is read on this path. The original producer stopped
        # writers before publication; this is an idempotent physical join.
        await self._run_process_join(deadline, False)

    async def _run_process_join(self, deadline, stop):
        task = asyncio.create_task(asyncio.to_thread(self._join_processes, deadline, stop))
        self._join_tasks.add(task)

        def completed(actual):
            self._join_tasks.discard(actual)
            if not actual.cancelled():
                actual.exception()

        task.add_done_callback(completed)
        await asyncio.shield(task)

    def _join_processes(self, deadline, stop):
        if not self._process_join_lock.acquire(timeout=max(0, deadline - time.monotonic())):
            raise TimeoutError("Original process join remains owned by another waiter")
        try:
            with self._condition:
                handles = tuple(self._process_handles)
            for handle in handles:
                if stop:
                    handle.stop_and_join(deadline)
                else:
                    handle.join(deadline)
                if handle._registered_shell:
                    self.process_registry.settled(handle.shell, deadline=deadline)
                if handle._registered_supervisor:
                    self.process_registry.settled(handle.supervisor, deadline=deadline)
                with self._condition:
                    self._process_handles = [current for current in self._process_handles if current is not handle]
        finally:
            self._process_join_lock.release()

    def reopen(self, epoch):
        with self._condition:
            if self._final:
                raise OwnershipRejected("final workspace writer gate cannot reopen")
            if epoch != self._epoch or self._tickets or self._process_handles or self._join_tasks:
                raise OwnershipRejected("Workspace barrier identity is stale or unsettled")
            self._closed = False


@contextmanager
def workspace_writer_scope(controller):
    token = _controller.set(controller)
    try:
        yield
    finally:
        _controller.reset(token)


def current_workspace_controller():
    return _controller.get()


def native_writer(func):
    """Guard the real synchronous body, including direct sandbox calls."""

    @wraps(func)
    def guarded(*args, **kwargs):
        controller = _controller.get()
        inherited = _ticket.get()
        if controller is None or (inherited is not None and inherited.controller is controller and not inherited.finished):
            return func(*args, **kwargs)
        ticket = controller.reserve()
        try:
            with ticket.scope():
                return func(*args, **kwargs)
        finally:
            ticket.finish()

    return guarded


async def run_native_writer(func, *args, **kwargs):
    controller = _controller.get()
    if controller is None:
        return await asyncio.to_thread(func, *args, **kwargs)
    ticket = controller.reserve()

    def native():
        try:
            with ticket.scope():
                return func(*args, **kwargs)
        finally:
            ticket.finish()

    task = asyncio.create_task(asyncio.to_thread(native))
    # Retain the real Task independently of the awaiter and retrieve failures.
    # Ticket release happens exclusively inside the real native body.
    controller._native_tasks.add(task)

    def complete(real_task):
        controller._native_tasks.discard(real_task)
        if not real_task.cancelled():
            real_task.exception()

    task.add_done_callback(complete)
    return await asyncio.shield(task)


def settled_workspace_activity(func):
    """Track the real coroutine through its complete cancellation/finally path.

    Unlike a native ticket, an activity ticket never grants nested file calls
    admission after the gate has closed. Those calls obtain their own tickets.
    """

    @wraps(func)
    async def tracked(*args, **kwargs):
        controller = _controller.get()
        if controller is None:
            return await func(*args, **kwargs)
        ticket = controller.reserve()
        actual_task = asyncio.current_task()
        with controller._condition:
            controller._native_tasks.add(actual_task)
        try:
            return await func(*args, **kwargs)
        finally:
            with controller._condition:
                controller._native_tasks.discard(actual_task)
            ticket.finish()

    return tracked


@contextmanager
def workspace_tool_scope(runtime):
    controller = _controller.get()
    if controller is None or controller.process_registry is None:
        yield
        return
    from deerflow.mcp.tasks.invocation import durable_tool_invocation_id

    identity = durable_tool_invocation_id(runtime)
    token = _tool_invocation.set(identity)
    try:
        yield
    finally:
        _tool_invocation.reset(token)


def original_workspace_tool_id():
    value = _tool_invocation.get()
    if value is None:
        raise OwnershipRejected("Workspace process requires original graph ExecutionInfo")
    return value


def workspace_tool_call(func):
    @wraps(func)
    def bound(runtime, *args, **kwargs):
        with workspace_tool_scope(runtime):
            return func(runtime, *args, **kwargs)

    return bound
