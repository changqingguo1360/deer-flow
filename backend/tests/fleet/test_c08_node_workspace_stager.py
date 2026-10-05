"""Actual NodeClient HTTP/NAS staging, with native census adapters scoped explicitly."""

import importlib
import importlib.util
import time
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission  # noqa: F401
from .test_c03_remote_agent_admission import owner_environment as owner_environment  # noqa: F401
from .test_c08_node_workspace_protocol import checkpoint_owner as checkpoint_owner  # noqa: F401
from .test_c08_node_workspace_protocol import publish_original


@pytest.mark.asyncio
@pytest.mark.parametrize("same_instance,cancel_copy,large_paths", ((False, False, False), (True, False, False), (False, True, False), (True, True, False), (False, False, True)))
async def test_actual_nodeclient_lost_prepared_reply_rebuild_reuses_candidate_and_bounded_pointer(checkpoint_owner, tmp_path, monkeypatch, same_instance, cancel_copy, large_paths):  # noqa: F811
    module_name = "deerflow_ecs_fleet.worker.workspace_publication"
    assert importlib.util.find_spec(module_name) is not None, "Original Node staged publication adapter is absent"
    module = importlib.import_module(module_name)
    from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.journal import AttemptJournal
    from deerflow_ecs_fleet.workspace import NASWorkspace

    item = checkpoint_owner
    components = tuple(str(level) + "d" * 179 for level in range(10))
    presented = tuple("/".join(("outputs", *components, f"file-{index:04d}")) for index in range(1000)) if large_paths else ()
    identity, epoch, teardown = await publish_original(item, presented_paths=presented)
    nas = NASWorkspace(item.env[3].config.nas_root, identity=item.env[3].config.nas_identity)
    output = nas.root / identity.user_id / identity.thread_id / "agents" / identity.agent_task_id / identity.attempt_id
    source = output / ".deer-flow/users" / identity.user_id / "threads" / identity.thread_id / "user-data"
    for category in ("workspace", "uploads", "outputs"):
        (source / category).mkdir(parents=True, exist_ok=True)
    (source / "outputs/result.txt").write_text("actual staged candidate")
    if large_paths:
        import os
        from dataclasses import asdict

        from deerflow_ecs_fleet.agent_workspace import canonical
        from deerflow_ecs_fleet.worker.journal import MAX_RECORD_BYTES

        assert len(canonical(asdict(identity))) > MAX_RECORD_BYTES
        fd = os.open(source / "outputs", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for component in components:
                os.mkdir(component, dir_fd=fd)
                child_fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child_fd
            for index in range(1000):
                leaf = os.open(f"file-{index:04d}", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
                os.close(leaf)
        finally:
            os.close(fd)
    lost, censuses, copies, recoveries = [], [], [], []
    transport = httpx.ASGITransport(app=item.env[4])

    class LostResponse(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            response = await transport.handle_async_request(request)
            if request.url.path.endswith("/workspace/prepared") and not lost:
                assert response.status_code == 200
                await response.aread()
                lost.append(True)
                raise httpx.ReadError("actual committed response dropped", request=request)
            return response

    async def native_census(grant, **kwargs):
        # Exact Docker/proc proof is deliberately a separate installed Linux gate.
        censuses.append(kwargs["request"]["state"])
        return {
            "container_id": "a" * 64,
            "started_at": "original",
            "image": "original",
            "launch_fingerprint": "b" * 64,
            "receipt": {"collector": {"pid_namespace": 77}, "runner": {"pid": 1, "ppid": 0, "start_ticks": 100, "state": "S", "uid": 65534}, "cgroup_digest": "c" * 64},
        }

    original_recover = AgentWorkspaceVersions.recover

    def counted_recover(self, *args, **kwargs):
        recoveries.append(True)
        return original_recover(self, *args, **kwargs)

    monkeypatch.setattr(AgentWorkspaceVersions, "recover", counted_recover)
    import asyncio
    import threading

    copying, release_copy = threading.Event(), threading.Event()
    original_seal = AgentWorkspaceVersions.seal

    def counted_seal(self, *args, **kwargs):
        copies.append(True)
        if cancel_copy:
            copying.set()
            assert release_copy.wait(3), "Native copy release fixture timed out"
        return original_seal(self, *args, **kwargs)

    monkeypatch.setattr(AgentWorkspaceVersions, "seal", counted_seal)
    journal = AttemptJournal(tmp_path / "journal")
    claim = {"kind": "agent", "attempt_id": identity.attempt_id, "token": item.accepted.token}
    record = {"claim": claim, "node_id": identity.node_id, "reported": False, "grant": item.grant}
    journal.save(record)
    async with httpx.AsyncClient(transport=LostResponse(), base_url="http://test") as http:
        client = NodeClient(gateway_url="http://test", credential=item.env[7].token, http_client=http, claim_kind="agent", compatibility={})
        client.node_id, client.session_id = identity.node_id, identity.node_session_id
        stager = module.AgentWorkspacePublication(client=client, containers=SimpleNamespace(quiesce_workspace=native_census), nas=nas, journal=journal)
        try:
            if cancel_copy:
                waiter = asyncio.create_task(stager.step(claim, item.grant, output_dir=output, record=record, deadline=time.monotonic() + 10))
                for _ in range(100):
                    if copying.is_set():
                        break
                    await asyncio.sleep(0.01)
                assert copying.is_set()
                waiter.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await waiter
                assert len(stager.pending_copies) == 1
                async with item.engine.connect() as connection:
                    assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_manifests"))).scalar_one() == 0
                release_copy.set()
                await asyncio.gather(*stager.pending_copies)
                assert not stager.pending_copies
            with pytest.raises(httpx.ReadError):
                await stager.step(claim, item.grant, output_dir=output, record=record, deadline=time.monotonic() + 10)
            pointer = journal.records()[0]["workspace_candidate"]
            assert set(pointer) == {"request_id", "request_digest", "barrier_epoch", "nonce", "manifest_id"}
            assert pointer["request_id"] == identity.request_id and pointer["barrier_epoch"] == epoch
            if large_paths:
                assert (journal.root / (identity.attempt_id + ".json")).stat().st_size < MAX_RECORD_BYTES
            # Rebuild only the service over the original live owner and durable
            # journal. No bootstrap, stop, replacement runner or second claim.
            rebuilt = stager if same_instance else module.AgentWorkspacePublication(client=client, containers=SimpleNamespace(quiesce_workspace=native_census), nas=nas, journal=journal)
            reloaded = journal.records()[0]
            assert await rebuilt.step(claim, item.grant, output_dir=output, record=reloaded, deadline=time.monotonic() + 10)
            assert reloaded["workspace_candidate"] == pointer and copies == [True]
            assert len(censuses) == (5 if cancel_copy else 4)
            assert len(recoveries) == (3 if cancel_copy else 2), "Each Node retry must read and verify the full fixed candidate descriptor"
            assert not rebuilt._copies, "Completed descriptor tasks must not accumulate across boundaries"
            async with item.engine.connect() as connection:
                assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_manifests"))).scalar_one() == 1
                assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_points"))).scalar_one() == 0
        finally:
            release_copy.set()
            if stager.pending_copies:
                await asyncio.gather(*stager.pending_copies, return_exceptions=True)
            await teardown.close()


@pytest.mark.asyncio
async def test_original_daemon_loop_retries_stage_transport_failure_while_watchdog_and_renew_remain_active(tmp_path):
    import asyncio
    import subprocess
    import sys

    from deerflow_ecs_fleet.worker.daemon import NodeDaemon

    child = None
    stopped = []
    steps, renews = [], []
    done = False
    claim = {"kind": "agent", "attempt_id": "original-attempt", "token": "original-token"}
    grant = {"authorized": True, "kind": "agent", "attempt_id": claim["attempt_id"], "process_ref": "fleet-" + claim["attempt_id"], "lease_seconds_remaining": 3, "execution_seconds_remaining": 3}

    class Containers:
        def bind_claim(self, value):
            assert value is claim

        async def launch(self, value, **kwargs):
            nonlocal child
            assert child is None, "Staging must never start a second runner"
            child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
            return {"State": {"Status": "running", "Running": True}}

        async def inspect(self, ref):
            return {"State": {"Running": not done and child.poll() is None, "ExitCode": 0}}

        async def stop(self, ref):
            if child is not None and child.poll() is None:
                child.terminate()
                child.wait(timeout=3)
            stopped.append(ref)
            return True

    class Client:
        node_id = "node"

        async def attempt(self, value, operation, **kwargs):
            if operation == "start":
                return grant
            if operation == "renew":
                renews.append(True)
                return {"stop": False, "lease_seconds_remaining": 3, "execution_seconds_remaining": 3}
            assert operation == "stopped" and child.poll() is not None
            return {"state": "succeeded"}

    class Stage:
        async def save_record(self, record):
            await asyncio.to_thread(daemon.journal.save, record)

        async def step(self, value, original, **kwargs):
            nonlocal done
            steps.append(True)
            assert value is claim and original is grant and child.poll() is None
            assert kwargs["deadline"] > time.monotonic()
            await asyncio.sleep(0.04)
            if len(steps) == 1:
                raise httpx.ReadError("real transport failure contract")
            done = True
            return True

    async def prepare(value, original):
        output = tmp_path / "original-output"
        output.mkdir()
        return output

    daemon = NodeDaemon(client=Client(), containers=Containers(), state_dir=tmp_path / "private", prepare_workspace=prepare, renew_seconds=0.02, safety_margin_seconds=0.01, poll_seconds=0.005)
    daemon.workspace_publications = Stage()
    daemon._ready = True
    try:
        result = await asyncio.wait_for(daemon.execute(claim), timeout=0.8)
        assert len(steps) == 2 and renews
        assert result["state"] == "succeeded" and child.poll() is not None and stopped
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            child.wait(timeout=3)


@pytest.mark.asyncio
async def test_actual_parked_stage_journal_save_cancel_cannot_overwrite_later_stop_record(tmp_path, monkeypatch):
    import asyncio
    import threading

    from deerflow_ecs_fleet.worker.journal import AttemptJournal
    from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication

    journal = AttemptJournal(tmp_path / "private")
    saving, release = threading.Event(), threading.Event()
    original_save = journal.save
    written = []

    def parked_save(record):
        if not record["reported"]:
            saving.set()
            assert release.wait(3)
        original_save(record)
        written.append(record["reported"])

    monkeypatch.setattr(journal, "save", parked_save)
    stager = AgentWorkspacePublication(client=None, containers=None, nas=None, journal=journal)
    save = getattr(stager, "save_record", None)
    assert callable(save), "C publication journal writes have no retained serial owner"
    first = asyncio.create_task(save({"claim": {"attempt_id": "original"}, "reported": False}))
    try:
        for _ in range(100):
            if saving.is_set():
                break
            await asyncio.sleep(0.01)
        assert saving.is_set()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        final = asyncio.create_task(save({"claim": {"attempt_id": "original"}, "reported": True, "stop_reason": "lease_lost"}))
        await asyncio.sleep(0.03)
        assert not final.done(), "Final record must wait for the actual earlier native save"
        release.set()
        await final
        assert written == [False, True]
        assert journal.records()[0]["reported"] is True
        assert not stager.pending_saves
    finally:
        release.set()
        if not first.done():
            await asyncio.gather(first, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("writer_kind", ("journal", "copy"))
async def test_original_agent_cli_repeated_cancel_keeps_flock_and_client_until_actual_journal_writer_settles(tmp_path, monkeypatch, writer_kind):
    import asyncio
    import fcntl
    import os
    import threading

    from deerflow_ecs_fleet.worker import __main__ as entry
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers
    from deerflow_ecs_fleet.worker.journal import AttemptJournal
    from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication

    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("native\n")
    credential = tmp_path / "credential"
    credential.write_text("df_fleet_native-control")
    credential.chmod(0o600)
    operator = tmp_path / "operator"
    operator.write_text("{}")
    operator.chmod(0o600)
    state = tmp_path / "state"
    entered, release = threading.Event(), threading.Event()
    services, saves, closed = [], [], []
    original_save, original_init = AttemptJournal.save, AgentWorkspacePublication.__init__

    def parked_save(self, record):
        entered.set()
        assert release.wait(3)
        original_save(self, record)

    def install_pending_owner(self, **kwargs):
        original_init(self, **kwargs)
        services.append(self)
        if writer_kind == "journal":
            saves.append(asyncio.create_task(self.save_record({"claim": {"attempt_id": "original", "kind": "agent"}, "reported": False})))
        else:
            from .test_c08_workspace import identity

            boundary = identity()
            output = self.nas.root / boundary.user_id / boundary.thread_id / "agents" / boundary.agent_task_id / boundary.attempt_id
            source = output / ".deer-flow" / "users" / boundary.user_id / "threads" / boundary.thread_id / "user-data"
            for category in ("workspace", "uploads", "outputs"):
                (source / category).mkdir(parents=True)
            (source / "outputs" / "result").write_bytes(b"actual late native candidate")

            def physical_copy():
                entered.set()
                assert release.wait(3)
                return self._candidate(boundary, {"input_limits": {"max_input_bytes": 1024}, "execution_profile": {"max_output_bytes": 1024}}, output)

            task = asyncio.create_task(asyncio.to_thread(physical_copy))
            self._copies["owned-native-copy"] = task
            saves.append(task)

    class Client:
        node_id = "native-node"
        session_id = "native-session"

        def __init__(self, **kwargs):
            self.calls = 0

        async def open_session(self):
            return {}

        async def heartbeat(self):
            self.calls += 1
            if self.calls == 2:
                asyncio.get_running_loop().call_soon(asyncio.current_task().cancel)
            return {"health": "online"}

        async def claim(self):
            await asyncio.sleep(0)
            return None

        async def close(self):
            closed.append(True)

    async def no_residual(self, node):
        return []

    monkeypatch.setattr(entry, "NodeClient", Client)
    monkeypatch.setattr(AgentContainers, "list_managed", no_residual)
    monkeypatch.setattr(AttemptJournal, "save", parked_save)
    monkeypatch.setattr(AgentWorkspacePublication, "__init__", install_pending_owner)
    settings = entry.WorkerSettings(kind="agent", agent_image="native-image", agent_config_file=operator, gateway_url="http://localhost", credential_file=credential, state_dir=state, nas_root=nas, nas_identity="native")
    worker = asyncio.create_task(entry.run_worker(settings))
    try:
        for _ in range(100):
            if entered.is_set():
                break
            await asyncio.sleep(0.01)
        assert entered.is_set()
        await asyncio.sleep(0.03)
        assert not closed, "Original CLI closed its control client while a native journal writer was alive"
        worker.cancel()
        await asyncio.sleep(0.03)
        assert not worker.done() and not closed
        other = os.open(state / ".daemon.lock", os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(other)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert closed == [True] and not services[0].pending_saves
        if writer_kind == "journal":
            assert services[0].journal.records()[0]["reported"] is False
        else:
            from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions

            from .test_c08_workspace import identity

            candidate = AgentWorkspaceVersions(services[0].nas, max_input_bytes=1024, max_output_bytes=1024).recover(identity())
            assert candidate is not None  # unaccepted late NAS candidate only
    finally:
        release.set()
        await asyncio.gather(*saves, return_exceptions=True)
        await asyncio.gather(worker, return_exceptions=True)
