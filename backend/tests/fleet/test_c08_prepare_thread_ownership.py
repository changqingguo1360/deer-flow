"""Actual native preparation and original worker shutdown; controlled RPC/containers, no Docker proof."""

import asyncio
import fcntl
import os
import stat
import threading

import pytest

from .test_c08_empty_initial_workspace import initial_workspace
from .test_c08_workspace import seal


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
@pytest.mark.parametrize("copy_failure", [False, True])
async def test_original_worker_shutdown_retains_native_prepare_and_flock(tmp_path, monkeypatch, accepted, copy_failure):
    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions
    from deerflow_ecs_fleet.worker import __main__ as entry
    from deerflow_ecs_fleet.worker import agent_containers as module
    from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceSnapshots

    item = initial_workspace(tmp_path, nonempty=True)
    spec = dict(item.spec, agent_task_id="task")
    claim = {"kind": "agent", "attempt_id": "child-attempt", "token": "test-private-token"}
    grant = {
        "authorized": True,
        "attempt_id": claim["attempt_id"],
        "process_ref": "fleet-child-attempt",
        "launch_spec": spec,
        "lease_seconds_remaining": 30,
        "execution_seconds_remaining": 30,
        "input_limits": {"max_input_bytes": 1024},
        "execution_profile": {"max_output_bytes": 1024, "image": "test-image"},
    }
    if accepted:
        item.snapshots.prepare(item.spec, item.attempt)
        store = AgentWorkspaceVersions(item.nas, max_input_bytes=1024, max_output_bytes=1024)
        manifest = seal(store, item.user_data, item.boundary)
        grant.update(accepted_workspace={"manifest": manifest.model_dump(mode="json"), "point_id": "point", "checkpoint_id": "checkpoint"}, nas_identity="native-empty")
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    events = []
    executions, daemons = [], []
    native_scope = threading.local()
    fsync = os.fsync

    def actual_fsync(fd):
        if copy_failure and getattr(native_scope, "active", False) and stat.S_ISREG(os.fstat(fd).st_mode):
            raise error
        return fsync(fd)

    monkeypatch.setattr(os, "fsync", actual_fsync)
    error = OSError("owned native copy failure")
    selected = "prepare_accepted" if accepted else "prepare"
    original = getattr(AgentWorkspaceSnapshots, selected)

    def blocked(self, *args, **kwargs):
        native_scope.active = True
        events.append("native-enter")
        entered.set()
        assert release.wait(5), "test-owned native release deadline"
        try:
            result = original(self, *args, **kwargs)
            events.append("marker-durable")
            return result
        finally:
            native_scope.active = False
            events.append("native-finished")
            finished.set()

    monkeypatch.setattr(AgentWorkspaceSnapshots, selected, blocked)

    class Driver(module.AgentContainers):
        async def list_managed(self, node_id):
            return []

        async def stop(self, ref):
            events.append("physical-stop")
            return True

        async def inspect(self, ref):
            return None

        async def launch(self, *args, **kwargs):
            raise AssertionError("cancelled preparation must not launch")

    class Client:
        node_id = "node"

        def __init__(self, **kwargs):
            self.claimed = False

        async def open_session(self):
            pass

        async def heartbeat(self):
            return {"health": "online"}

        async def claim(self):
            if not self.claimed:
                self.claimed = True
                return claim
            return None

        async def attempt(self, original_claim, action, **kwargs):
            if action == "start":
                return grant
            assert action == "stopped"
            events.append("stopped-report")
            return {"state": "recovery_required"}

        async def close(self):
            events.append("client-close")

    class ObservedDaemon(entry.NodeDaemon):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            daemons.append(self)

        async def execute(self, original_claim):
            try:
                return await super().execute(original_claim)
            except BaseException as failure:
                executions.append(failure)
                raise

    monkeypatch.setattr(entry, "NodeDaemon", ObservedDaemon)
    monkeypatch.setattr(module, "AgentContainers", Driver)
    monkeypatch.setattr(entry, "NodeClient", Client)
    callbacks = {}
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda sig, callback: callbacks.setdefault(sig, callback))
    monkeypatch.setattr(loop, "remove_signal_handler", lambda sig: callbacks.pop(sig, None))
    credential, config = tmp_path / "credential", tmp_path / "config"
    credential.write_text("test-private-token")
    config.write_text("{}")
    credential.chmod(0o600)
    config.chmod(0o600)
    settings = entry.WorkerSettings(
        kind="agent",
        agent_image="test-image",
        agent_config_file=config,
        gateway_url="http://test.invalid",
        credential_file=credential,
        state_dir=tmp_path / "worker-state",
        nas_root=item.nas.root,
        nas_identity="native-empty",
        poll_seconds=0.01,
        safety_margin_seconds=0.1,
    )
    worker = asyncio.create_task(entry.run_worker(settings))
    lock_fd = None
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        callbacks[entry.signal.SIGTERM]()  # original ordinary shutdown event
        await asyncio.sleep(0.05)
        worker.cancel()
        await asyncio.sleep(0.05)
        worker.cancel()  # cancellation while original run gather is joining execute
        await asyncio.sleep(0.05)
        lock_fd = os.open(settings.state_dir / ".daemon.lock", os.O_RDWR)
        with pytest.raises(BlockingIOError):
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert not worker.done() and not finished.is_set()
        assert daemons[0]._active == {claim["attempt_id"]}
        assert "physical-stop" not in events and "stopped-report" not in events and "client-close" not in events
    finally:
        release.set()
        result = (await asyncio.gather(worker, return_exceptions=True))[0]
        if lock_fd is not None:
            os.close(lock_fd)
        assert await asyncio.to_thread(finished.wait, 2)
    assert isinstance(result, asyncio.CancelledError)
    assert events.index("native-finished") < events.index("physical-stop") < events.index("stopped-report") < events.index("client-close")
    assert daemons[0]._active == set() and len(executions) == 1
    if copy_failure:
        assert isinstance(executions[0], BaseExceptionGroup)
        assert isinstance(executions[0].exceptions[0], asyncio.CancelledError)
        assert executions[0].exceptions[1] is error
        assert "marker-durable" not in events
    else:
        assert isinstance(executions[0], asyncio.CancelledError)
        assert events.index("marker-durable") < events.index("native-finished")
    assert len(list((settings.state_dir / "prepared-workspaces").glob("*.json"))) == (0 if copy_failure else 1)
    fd = os.open(settings.state_dir / ".daemon.lock", os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(fd)
