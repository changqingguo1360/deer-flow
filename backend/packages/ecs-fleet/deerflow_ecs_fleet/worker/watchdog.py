"""A worker's monotonic lease deadline is independent of control-plane timers."""

import asyncio
import math
import time


class LeaseWatchdog:
    def __init__(self, *, stop, lease_seconds: float, safety_margin_seconds: float = 5):
        if not math.isfinite(lease_seconds) or not 0 <= safety_margin_seconds < lease_seconds:
            raise ValueError("Invalid local lease interval")
        self.stop = stop
        self.margin = safety_margin_seconds
        self.deadline = time.monotonic() + lease_seconds - self.margin
        self.changed = asyncio.Event()
        self.expired = False

    def renew(self, lease_seconds: float, *, request_started_at: float | None = None) -> bool:
        if self.expired or time.monotonic() >= self.deadline:
            return False
        if not math.isfinite(lease_seconds) or lease_seconds <= self.margin:
            return False
        # Base the new bound on request send time: a delayed reply cannot add
        # its network round-trip time to the server's remaining lease.
        self.deadline = (request_started_at if request_started_at is not None else time.monotonic()) + lease_seconds - self.margin
        self.changed.set()
        return True

    async def run(self) -> bool:
        try:
            while True:
                self.changed.clear()
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    self.expired = True
                    return await self.stop()
                try:
                    await asyncio.wait_for(self.changed.wait(), timeout=remaining)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            self.expired = True
            await asyncio.shield(self.stop())
            raise
