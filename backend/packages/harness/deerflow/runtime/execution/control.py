"""Host-neutral control injected into the existing run manager/store lifecycle."""

from typing import Any, Protocol


class AgentRunControl(Protocol):
    async def request_cancel(self, run_id: str, *, action: str) -> Any | None:
        """Return None only when this host does not own the execution backend."""
        ...

    async def wait_stopped(self, run_id: str, *, disconnected=None) -> bool | None:
        """None explicitly delegates Local/B; unavailable observation is False.

        True requires the original execution's durable physical stop. Backend
        classification shares the host's bounded observation lifecycle.
        """
        ...
