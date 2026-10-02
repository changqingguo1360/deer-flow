"""Private, immutable execution mutation authority; never graph configuration."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Protocol


class OwnershipRejected(RuntimeError):
    """Nonretryable rejection of an execution's durable mutation authority."""

    retryable = False


@dataclass(frozen=True, repr=False)
class RemoteMutationContext:
    user_id: str
    thread_id: str
    run_id: str
    agent_task_id: str
    generation: int
    node_id: str
    node_session_id: str
    attempt_id: str
    owner_worker_id: str
    token_stamp: str
    launch_spec_digest: str


@dataclass(frozen=True)
class MutationTarget:
    run_id: str | None = None
    thread_id: str | None = None
    user_id: str | None = None
    status: str | None = None
    event_types: tuple[str, ...] = ()
    error: str | None = None
    stop_reason: str | None = None


_current_mutation_context: ContextVar[RemoteMutationContext | None] = ContextVar("remote_mutation_context", default=None)


def current_remote_mutation_context():
    return _current_mutation_context.get()


@contextmanager
def remote_mutation_scope(context: RemoteMutationContext):
    if not isinstance(context, RemoteMutationContext):
        raise OwnershipRejected("Invalid remote mutation context")
    token = _current_mutation_context.set(context)
    try:
        yield
    finally:
        _current_mutation_context.reset(token)


class MutationCapability(Protocol):
    context: RemoteMutationContext

    async def validate_async(self, session: Any, *, context: RemoteMutationContext | None, operation: str, targets: tuple[MutationTarget, ...]) -> None: ...
    def validate_sync(self, session: Any, *, context: RemoteMutationContext | None, operation: str, targets: tuple[MutationTarget, ...]) -> None: ...
    async def validate_cursor(self, cursor: Any, *, context: RemoteMutationContext | None, operation: str, targets: tuple[MutationTarget, ...]) -> None: ...


async def validate_mutation(capability, session, operation, **target):
    if capability is not None:
        await capability.validate_async(session, context=current_remote_mutation_context(), operation=operation, targets=(MutationTarget(**target),))


def reject_remote_operation(capability):
    if capability is not None:
        raise OwnershipRejected("Remote mutation operation is unsupported")
