"""Native original observer ordering; POSIX child is not a Linux supervisor proof."""

import asyncio
import subprocess
import sys
import time
from contextlib import AsyncExitStack, nullcontext
from types import SimpleNamespace

import pytest

from app.fleet.agent_control import OriginalAgentCancellation
from app.fleet.runner_context import _AgentResourceTeardown
from deerflow.runtime.execution.mutation_context import OwnershipRejected
from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, run_native_writer, workspace_writer_scope


@pytest.mark.asyncio
async def test_original_cancel_observer_stops_retained_writer_before_executor_cancel(monkeypatch):
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 5
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(5)"])
    stopped = []
    started = asyncio.Event()
    deadline = controller.execution_deadline

    class OriginalHandle:
        def request_stop(self, received_deadline):
            assert received_deadline == deadline
            stopped.append(process.pid)
            process.terminate()
            # The native POSIX probe waits for actual exit. Production sends to
            # its original supervisor, whose sole receipt reader remains bash.
            process.wait(timeout=1)

    handle = OriginalHandle()
    controller.retain_process(handle)
    context = SimpleNamespace(run_id="original-run")
    capability = SimpleNamespace(context=context, retain_cancellation_deadline=lambda value: None)
    teardown = _AgentResourceTeardown(AsyncExitStack(), nullcontext, context)

    async def observe(*_):
        return {"action": "interrupt", "monotonic_deadline": deadline}

    async def signal(run_id, *, action):
        assert (run_id, action) == (context.run_id, "interrupt")
        assert stopped == [process.pid], "Executor cancellation preceded original supervised writer stop"
        assert process.poll() is not None
        with pytest.raises(OwnershipRejected):
            controller.reserve()

    monkeypatch.setattr("app.fleet.agent_control.observe_original_control", observe)
    cancellation = OriginalAgentCancellation(None, capability, SimpleNamespace(signal_execution_cancel=signal), teardown, controller)

    def native_body():
        process.wait(timeout=6)

    async def actual_writer():
        with workspace_writer_scope(controller):
            started.set()
            await run_native_writer(native_body)

    writer = asyncio.create_task(actual_writer())
    observer = None
    try:
        await started.wait()
        await asyncio.sleep(0)
        assert controller.active_count == 1
        observer = asyncio.create_task(cancellation.observe())
        async with asyncio.timeout(2):
            await observer
            await writer
        assert controller.active_count == 0
        assert controller.barrier_epoch == 1
        assert not teardown._phases
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=2)
        await writer
        if observer is not None and not observer.done():
            observer.cancel()
            await asyncio.gather(observer, return_exceptions=True)
        assert writer.done() and not controller._native_tasks


@pytest.mark.asyncio
async def test_cancel_late_retained_original_pipe_stops_without_reading_receipt():
    import os
    import threading

    from deerflow.runtime.execution.workspace_process import SupervisedCommand

    control_read, control_write = os.pipe()
    receipt_read, receipt_write = os.pipe()
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import os,sys; reader=os.fdopen(int(sys.argv[1]),'rb'); first=reader.readline(); second=reader.readl"
            'ine(); sys.exit(0 if first == b\'{"args":[],"env":null,"pids_limit":1}\\n\' and second == b\'{"operation":"stop"}\\n\' else 1)',
            str(control_read),
        ],
        pass_fds=(control_read,),
    )
    os.close(control_read)
    handle = SupervisedCommand.__new__(SupervisedCommand)
    handle.process, handle._control_write, handle._receipt_read = process, control_write, receipt_read
    handle._send_lock, handle._stop_requested, handle._closed = threading.RLock(), False, False
    handle.pipe_drains = ()
    handle._initial_payload_sent, handle._pending_stop_deadline = False, None
    controller = WorkspaceWriterController()
    deadline = time.monotonic() + 2
    try:
        os.write(receipt_write, b"sole-native-reader")
        controller.close_admission(final=True)
        controller.request_process_stop(deadline=deadline)
        # Retention occurs after the first signal snapshot. The actual original
        # request_stop implementation must still signal this owned pipe.
        controller.retain_process(handle)
        assert handle._pending_stop_deadline == deadline and not handle._stop_requested
        with handle._send_lock:
            handle._send({"args": [], "env": None, "pids_limit": 1})
            handle._initial_payload_sent = True
            handle.request_stop(handle._pending_stop_deadline)
        await asyncio.to_thread(process.wait, timeout=1)
        assert process.returncode == 0
        assert controller._process_handles == [handle]
        assert controller.unsettled and controller.barrier_epoch == 1
        assert os.read(receipt_read, 18) == b"sole-native-reader"
        controller.request_process_stop(deadline=deadline)
        assert controller._process_handles == [handle]
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=2)
        handle.join(time.monotonic() + 2)
        os.close(receipt_write)


@pytest.mark.asyncio
async def test_partial_barrier_keeps_ticket_completion_and_epoch_reopen_order():
    controller = WorkspaceWriterController()
    ticket = controller.reserve()
    barrier = asyncio.create_task(controller.close_and_wait(deadline=time.monotonic() + 2))
    try:
        await asyncio.sleep(0)
        assert not barrier.done() and controller.active_count == 1
        assert controller._process_stop_deadline is None
        with pytest.raises(OwnershipRejected):
            controller.reserve()
        ticket.finish()
        epoch = await barrier
        assert epoch == 1 and controller.active_count == 0
        controller.reopen(epoch)
        controller.close_admission(final=True)
        controller.close_admission(final=True)
        assert controller.barrier_epoch == 2
        with pytest.raises(OwnershipRejected):
            controller.reopen(2)
    finally:
        ticket.finish()
        await barrier
