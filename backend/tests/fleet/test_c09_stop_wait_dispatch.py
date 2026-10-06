"""Original manager dispatch probes; controlled reads do not claim PG faults."""

import asyncio

import pytest
from sqlalchemy.exc import OperationalError

from app.fleet.agent_control import FleetAgentRunControl
from deerflow.runtime.runs.manager import RunManager

from .test_c09_stop_wait_budget import StopReads


class ClassifiedReads(StopReads):
    def __init__(self, *, backend="fleet", placed=True, attempted=True, **kwargs):
        super().__init__(**kwargs)
        self.backend, self.placed, self.attempted = backend, placed, attempted

    def __call__(self):
        index = self.reads
        session = super().__call__()
        execute = session.execute

        async def classified(statement):
            result = await execute(statement)
            if index == 0:
                row = result.one_or_none()[:7]
                if not self.attempted:
                    row = (None, None, row[2], None, None, row[5], None)
                row += ({"execution_backend": self.backend}, "run" if self.placed else None, "pending", "queued" if self.placed else None)
                return type("Result", (), {"one_or_none": lambda self: row})()
            return result

        session.execute = classified
        return session


class DispatchStore:
    def __init__(self, reads, *, lookup="normal"):
        self._agent_run_control = FleetAgentRunControl(reads)
        self.reads, self.lookup, self.lookups = reads, lookup, 0

    async def get(self, run_id):
        self.lookups += 1
        if self.lookup == "stall":
            await asyncio.Event().wait()
        if self.lookup == "error":
            raise OperationalError("redundant dispatch", {}, RuntimeError("unavailable"))
        return {"kwargs": {"execution_backend": self.reads.backend}}


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup", ["stall", "error"])
async def test_original_manager_dispatch_avoids_unbounded_store_get(lookup):
    reads = ClassifiedReads(remaining=-1)
    store = DispatchStore(reads, lookup=lookup)
    async with asyncio.timeout(0.3):
        assert await RunManager(store=store).wait_execution_stopped("run") is False
    assert store.lookups == 0 and reads.reads == 1 and reads.active == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["local", "job"])
async def test_original_manager_explicit_local_b_fallthrough(backend):
    reads = ClassifiedReads(backend=backend, placed=False, attempted=False)
    store = DispatchStore(reads)
    assert await RunManager(store=store).wait_execution_stopped("run") is None
    assert store.lookups == 0 and reads.reads == 1 and reads.active == 0


@pytest.mark.asyncio
async def test_original_manager_remote_placement_without_attempt_is_pending():
    reads = ClassifiedReads(backend="local", placed=True, attempted=False)
    store = DispatchStore(reads)
    assert await RunManager(store=store).wait_execution_stopped("run") is False
    assert store.lookups == 0 and reads.reads == 1 and reads.active == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("stopped", [False, True])
async def test_original_manager_only_durable_stop_confirms(stopped):
    reads = ClassifiedReads(remaining=-1, stopped=stopped)
    store = DispatchStore(reads)
    assert await RunManager(store=store).wait_execution_stopped("run") is stopped
    assert store.lookups == 0 and reads.active == 0


@pytest.mark.asyncio
async def test_original_manager_unavailable_classification_stays_pending():
    reads = ClassifiedReads(failure=True)
    store = DispatchStore(reads)
    assert await RunManager(store=store).wait_execution_stopped("run") is False
    assert store.lookups == 0 and reads.active == 0


@pytest.mark.asyncio
async def test_original_manager_external_cancel_propagates():
    reads = ClassifiedReads(stall=(0, "query"))
    store = DispatchStore(reads)
    task = asyncio.create_task(RunManager(store=store).wait_execution_stopped("run"))
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
