"""Trusted infrastructure contract for same-transaction execution writes."""

from typing import Any, Protocol


class ExecutionWriteFence(Protocol):
    async def validate(self, cursor: Any, *, thread_id: str, operation: str) -> None:
        """Lock and validate execution ownership using the writer's transaction."""
