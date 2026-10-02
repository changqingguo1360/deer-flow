"""Host-bound active transactions for adapted memory and extensions.

Every yielded session is inside an explicit transaction, already holding the
original execution locks. This API never grants terminal SQL authority.
"""

from contextlib import asynccontextmanager, contextmanager

from .mutation_context import MutationTarget, OwnershipRejected, current_remote_mutation_context


class BoundMutationTransactions:
    def __init__(self, capability, *, operation, session_factory=None, sync_session_factory=None):
        if operation not in {"memory.write", "extension.write"}:
            raise ValueError("Unsupported adapted transaction domain")
        self._capability = capability
        self._operation = operation
        self._sf = session_factory
        self._sync_sf = sync_session_factory

    @property
    def context(self):
        return self._capability.context

    def capture(self, *, user_id=None, thread_id=None):
        if current_remote_mutation_context() != self.context:
            raise OwnershipRejected("Adapted mutation requires original context")
        if any(value is not None and value != getattr(self.context, name) for name, value in (("user_id", user_id), ("thread_id", thread_id))):
            raise OwnershipRejected("Adapted mutation target rejected")
        return self.context

    def _target(self, user_id, thread_id):
        self.capture(user_id=user_id, thread_id=thread_id)
        return (MutationTarget(user_id=user_id, thread_id=thread_id, run_id=self.context.run_id),)

    @asynccontextmanager
    async def async_transaction(self, *, user_id=None, thread_id=None):
        targets = self._target(user_id, thread_id)
        if self._sf is None:
            raise OwnershipRejected("Adapted async transaction unavailable")
        async with self._sf() as session, session.begin():
            await self._capability.validate_async(session, context=self.context, operation=self._operation, targets=targets)
            yield session
            await session.flush()
            await self._capability.validate_async(session, context=self.context, operation=self._operation, targets=targets)

    @contextmanager
    def sync(self, *, user_id=None, thread_id=None):
        targets = self._target(user_id, thread_id)
        if self._sync_sf is None:
            raise OwnershipRejected("Adapted sync transaction unavailable")
        with self._sync_sf() as session, session.begin():
            self._capability.validate_sync(session, context=self.context, operation=self._operation, targets=targets)
            yield session
            session.flush()
            self._capability.validate_sync(session, context=self.context, operation=self._operation, targets=targets)
