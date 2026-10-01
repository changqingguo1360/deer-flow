"""Trusted execution selection and transactional admission, independent of app/Fleet."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@dataclass(frozen=True)
class RunExecutionParameters:
    thread_id: str
    assistant_id: str | None
    user_id: str
    graph_input: Any
    normalized_config: dict
    stream_modes: tuple[str, ...]
    stream_subgraphs: bool
    interrupt_before: tuple[str, ...] | str | None
    interrupt_after: tuple[str, ...] | str | None
    public_kwargs: dict
    model_name: str | None = None


class RunAdmissionParticipant(Protocol):
    async def prepare(self, session: AsyncSession) -> None: ...
    async def insert(self, session: AsyncSession, admitted_run: dict) -> None: ...
    async def validate_reuse(self, session: AsyncSession, stored_run: dict) -> None: ...


@dataclass(frozen=True)
class ExecutionPlan:
    public_kwargs: dict
    store_only: bool = False
    participant: RunAdmissionParticipant | None = None

    def __post_init__(self):
        if self.store_only != (self.participant is not None):
            raise ValueError("Remote execution requires a transactional admission participant")


class ExecutionBackend(Protocol):
    def plan(self, parameters: RunExecutionParameters) -> ExecutionPlan: ...


class RunAdmissionUnitOfWork:
    """Own exactly one SQL session/transaction including all extension writes."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session:
            async with session.begin():
                yield session
