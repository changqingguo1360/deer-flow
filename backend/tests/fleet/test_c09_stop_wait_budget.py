"""Stop-wait budget probes; these session doubles do not claim real PG STOP."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import OperationalError

from app.fleet import agent_control


class StopReads:
    def __init__(self, *, stall=None, remaining=2, failure=False, stopped=False):
        self.stall, self.failure, self.stopped = stall, failure, stopped
        self.now = datetime.now(UTC)
        self.deadline = self.now + timedelta(seconds=remaining)
        self.reads = 0
        self.active = 0
        self.entered = asyncio.Event()

    def __call__(self):
        owner = self
        index = owner.reads
        owner.reads += 1

        class Session:
            async def phase(self, phase):
                if owner.stall == (index, phase):
                    owner.entered.set()
                    await asyncio.Event().wait()

            async def __aenter__(self):
                await self.phase("enter")
                owner.active += 1
                self.acquired = True
                return self

            async def __aexit__(self, *args):
                await self.close()

            async def close(self):
                try:
                    await self.phase("exit")
                finally:
                    if getattr(self, "acquired", False):
                        owner.active -= 1

            async def execute(self, statement):
                await self.phase("query")
                if owner.failure:
                    raise OperationalError("controlled read", {}, RuntimeError("unavailable"))
                row = ("original", 1, owner.now, owner.deadline, owner.deadline, owner.now, owner.now if owner.stopped else None, {"execution_backend": "fleet"}, "run") if index == 0 else (owner.now if owner.stopped else None, 1, owner.now)
                return type("Result", (), {"one_or_none": lambda self: row})()

        return Session()


@pytest.fixture(autouse=True)
def short_observation(monkeypatch):
    monkeypatch.setattr(agent_control, "_STOP_READ_SECONDS", 0.05, raising=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("read", [0, 1], ids=["initial", "poll"])
@pytest.mark.parametrize("phase", ["enter", "query", "exit"])
async def test_stop_wait_bounds_entire_read_lifecycle(read, phase):
    reads = StopReads(stall=(read, phase))
    # A separate test supervisor proves the original implementation overran its
    # observation bound without leaving a hanging request in the test process.
    async with asyncio.timeout(0.3):
        assert await agent_control.FleetAgentRunControl(reads).wait_stopped("run") is False
    assert reads.entered.is_set() and reads.active == 0


@pytest.mark.asyncio
async def test_stop_wait_clips_poll_to_immutable_remaining_budget():
    reads = StopReads(stall=(1, "query"), remaining=0.015)
    async with asyncio.timeout(0.04):
        assert await agent_control.FleetAgentRunControl(reads).wait_stopped("run") is False
    assert reads.active == 0


@pytest.mark.asyncio
async def test_stop_wait_disconnect_during_blocked_read():
    reads = StopReads(stall=(1, "query"))

    async def disconnected():
        return reads.entered.is_set()

    async with asyncio.timeout(0.3):
        assert await agent_control.FleetAgentRunControl(reads).wait_stopped("run", disconnected=disconnected) is False
    assert reads.entered.is_set() and reads.active == 0


@pytest.mark.asyncio
async def test_stop_wait_checks_disconnect_between_reads():
    reads = StopReads()
    calls = 0

    async def disconnected():
        nonlocal calls
        calls += 1
        return calls >= 2

    async with asyncio.timeout(0.3):
        assert await agent_control.FleetAgentRunControl(reads).wait_stopped("run", disconnected=disconnected) is False
    assert reads.reads == 1 and reads.active == 0


@pytest.mark.asyncio
async def test_stop_wait_unavailable_query_is_pending():
    reads = StopReads(failure=True)
    assert await agent_control.FleetAgentRunControl(reads).wait_stopped("run") is False
    assert reads.active == 0


@pytest.mark.asyncio
async def test_stop_wait_preserves_external_cancellation():
    reads = StopReads(stall=(0, "query"))
    task = asyncio.create_task(agent_control.FleetAgentRunControl(reads).wait_stopped("run"))
    try:
        await asyncio.wait_for(reads.entered.wait(), 0.3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled() and reads.active == 0


@pytest.mark.asyncio
async def test_stop_wait_durable_original_stop_survives_expired_budget():
    reads = StopReads(remaining=-1, stopped=True)
    assert await agent_control.FleetAgentRunControl(reads).wait_stopped("run") is True
    assert reads.active == 0
