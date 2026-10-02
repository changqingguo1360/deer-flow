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


def validate_mutation_sync(capability, session, operation, **target):
    if capability is not None:
        capability.validate_sync(session, context=current_remote_mutation_context(), operation=operation, targets=(MutationTarget(**target),))


def require_definition_mutation_scope(runtime):
    """Reject lost private resources before a remote tool can fall back to files."""
    from deerflow.persistence.agent_definition_context import get_scoped_definition_stores

    stores = get_scoped_definition_stores()
    resources = [getattr(runtime, "store", None), *(stores or ())]
    contexts = [getattr(getattr(resource, "_mutation_capability", None), "context", None) for resource in resources]
    bound = [context for context in contexts if isinstance(context, RemoteMutationContext)]
    current = current_remote_mutation_context()
    if not bound and current is None:
        return False
    if not bound or any(context != current for context in bound) or stores is None:
        raise OwnershipRejected("Remote definition mutation scope is missing or inconsistent")
    if any(not isinstance(getattr(getattr(store, "_mutation_capability", None), "context", None), RemoteMutationContext) for store in stores):
        raise OwnershipRejected("Remote definitions require bound database stores")
    return True
