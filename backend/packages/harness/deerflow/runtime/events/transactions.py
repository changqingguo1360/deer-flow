"""Optional participants in the original event writer transaction."""

from __future__ import annotations

from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession


class RunEventTransactionParticipant(Protocol):
    """Add private records using the event writer's session, without committing.

    Implementations must not publish externally or create another transaction.
    Raising aborts both the event and participant records. The writer's existing
    post-SQL authority check still runs before the shared commit.
    """

    async def insert(self, session: AsyncSession, *, event_id: int, record: dict) -> None: ...


class RunEventSequenceFloor(Protocol):
    """Optional retained highwater, read under the original thread sequence lock.

    A participant may implement this when its private pointers survive host
    retention. Local stores and participants without a floor remain unchanged.
    """

    async def sequence_floor(self, session: AsyncSession, *, thread_id: str) -> int: ...
