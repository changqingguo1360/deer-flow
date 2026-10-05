"""Native source contracts; actual Linux containment is a separate image gate."""

import pytest


def process(pid, ppid, ticks=None):
    return {"pid": pid, "ppid": ppid, "start_ticks": ticks or pid * 10, "pid_namespace": 77, "state": "S"}


def registry():
    return [{"pid": 10, "start_ticks": 100, "role": "supervisor", "source_digest": "a" * 64, "tool_execution_id": "b" * 64}]


@pytest.mark.parametrize("module_name", ("deerflow.runtime.execution.workspace_supervisor",))
@pytest.mark.parametrize("reused", (False, True))
def test_owned_signal_uses_bound_pidfd_after_identity_read(module_name, reused, monkeypatch):
    import importlib
    import signal

    module = importlib.import_module(module_name)
    opened, sent, closed = [], [], []
    monkeypatch.setattr(module.os, "pidfd_open", lambda pid, flags=0: opened.append(pid) or 123, raising=False)
    monkeypatch.setattr(module.os, "close", lambda fd: closed.append(fd))
    monkeypatch.setattr(module.signal, "pidfd_send_signal", lambda fd, sig, *args: sent.append((fd, sig)), raising=False)
    monkeypatch.setattr(module, "process_identity", lambda pid: process(pid, 10, 999 if reused else 110))
    if hasattr(module, "process_namespace"):
        monkeypatch.setattr(module, "process_namespace", lambda pid: 77)
    if reused:
        with pytest.raises(ValueError):
            module.signal_owned(11, 110, signal.SIGKILL, pid_namespace=77)
        assert sent == []
    else:
        module.signal_owned(11, 110, signal.SIGKILL, pid_namespace=77)
        assert sent == [(123, signal.SIGKILL)]
    assert opened == [11] and closed == [123]


def test_collector_disappearing_proc_entries_reject_incomplete_census(monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from deerflow_ecs_fleet.worker import workspace_collector as module

    @contextmanager
    def entries(path):
        yield iter([SimpleNamespace(name="10"), SimpleNamespace(name="11")])

    def gone(pid):
        raise FileNotFoundError()

    monkeypatch.setattr(module.os, "scandir", entries)
    monkeypatch.setattr(module, "process_identity", gone)
    with pytest.raises(ValueError, match="Incomplete"):
        module.process_census(1)


def test_collector_kernel_probe_requires_subreaper_and_pidfd(monkeypatch):
    from types import SimpleNamespace

    from deerflow_ecs_fleet.worker import workspace_collector as module

    calls, closed = [], []
    monkeypatch.setattr(module.sys, "platform", "linux")
    monkeypatch.setattr(module.ctypes, "CDLL", lambda *args, **kwargs: SimpleNamespace(prctl=lambda option, *args: calls.append(option) or 0))
    monkeypatch.setattr(module.os, "pidfd_open", lambda pid, flags=0: calls.append(("pidfd", pid)) or 123, raising=False)
    monkeypatch.setattr(module.signal, "pidfd_send_signal", lambda fd, sig, *args: calls.append(("signal", fd, sig)), raising=False)
    monkeypatch.setattr(module.os, "close", closed.append)
    module.kernel_capability()
    assert 36 in calls and 4 in calls
    assert ("pidfd", module.os.getpid()) in calls and ("signal", 123, 0) in calls
    assert closed == [123]


def test_collector_kernel_probe_never_advertises_unsupported_prctl(monkeypatch):
    from types import SimpleNamespace

    from deerflow_ecs_fleet.worker import workspace_collector as module

    monkeypatch.setattr(module.sys, "platform", "linux")
    monkeypatch.setattr(module.ctypes, "CDLL", lambda *args, **kwargs: SimpleNamespace(prctl=lambda *args: -1))
    with pytest.raises(ValueError):
        module.kernel_capability()


@pytest.mark.parametrize("pidfd_fails", (False, True))
def test_kernel_probe_restores_original_runner_flags_even_on_failure(monkeypatch, pidfd_fails):
    from types import SimpleNamespace

    from deerflow_ecs_fleet.worker import workspace_collector as module

    flags = {"dumpable": 1, "subreaper": 1}

    def actual_prctl(option, value, *args):
        if option == 3:
            return flags["dumpable"]
        if option == 37:
            value._obj.value = flags["subreaper"]
            return 0
        flags["dumpable" if option == 4 else "subreaper"] = value
        return 0

    def open_handle(pid, flags=0):
        if pidfd_fails:
            raise PermissionError("kernel fixture refuses pidfd")
        return 123

    monkeypatch.setattr(module.sys, "platform", "linux")
    monkeypatch.setattr(module.ctypes, "CDLL", lambda *args, **kwargs: SimpleNamespace(prctl=actual_prctl))
    monkeypatch.setattr(module.os, "pidfd_open", open_handle, raising=False)
    monkeypatch.setattr(module.signal, "pidfd_send_signal", lambda *args: None, raising=False)
    monkeypatch.setattr(module.os, "close", lambda fd: None)
    if pidfd_fails:
        with pytest.raises(PermissionError):
            module.kernel_capability()
    else:
        module.kernel_capability()
    assert flags == {"dumpable": 1, "subreaper": 1}


def test_fixed_installed_collector_copy_must_match_packaged_source(tmp_path, monkeypatch):
    from pathlib import Path

    from deerflow_ecs_fleet.worker import workspace_collector as module

    path = tmp_path / "trusted-collector.py"
    original = Path(module.__file__).read_bytes()
    path.write_bytes(original)
    monkeypatch.setattr(module, "COLLECTOR_PATH", str(path), raising=False)
    assert module.installed_collector_bytes() == original
    path.write_bytes(original + b"\n# changed fixed entrypoint\n")
    with pytest.raises(ValueError):
        module.installed_collector_bytes()


def test_fixed_installed_collector_rejects_symlink(tmp_path, monkeypatch):
    from pathlib import Path

    from deerflow_ecs_fleet.worker import workspace_collector as module

    path = tmp_path / "collector.py"
    path.symlink_to(Path(module.__file__))
    monkeypatch.setattr(module, "COLLECTOR_PATH", str(path), raising=False)
    with pytest.raises(OSError):
        module.installed_collector_bytes()


def collector_request():
    import json
    from dataclasses import asdict

    from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity

    identity = WorkspaceBoundaryIdentity(
        request_id="original-request",
        user_id="u",
        thread_id="t",
        run_id="r",
        agent_task_id="task",
        generation=1,
        attempt_id="attempt",
        launch_spec_digest="sha256:" + "a" * 64,
        node_id="node",
        node_session_id="session",
        owner_worker_id="fleet-agent:attempt",
        token_stamp="b" * 64,
        checkpoint_id="checkpoint",
        kind="partial",
        publication_key="outputs",
        presented_paths=(),
        source_workspace_version="original",
        process_ref="fleet-attempt",
    )
    return {
        "nonce": "c" * 64,
        "request_digest": identity.request_digest,
        "identity": json.loads(json.dumps(asdict(identity))),
        "barrier_epoch": 1,
        "processes": [],
        "pids_limit": 128,
        "supervisor_digest": "a" * 64,
        "timeout_seconds": 1,
        "workload_user": "65534:65534",
        "helper_user": "0:0",
    }


def test_collector_receipt_request_binds_full_private_original_identity():
    import json

    from deerflow_ecs_fleet.worker import workspace_collector as module

    value = collector_request()
    assert module.parse_request(json.dumps(value).encode() + b"\n") == value
    value["identity"]["token_stamp"] = "d" * 64
    with pytest.raises(ValueError):
        module.parse_request(json.dumps(value).encode() + b"\n")


def test_collector_private_request_rejects_duplicate_keys():
    import json

    from deerflow_ecs_fleet.worker import workspace_collector as module

    raw = json.dumps(collector_request()).encode()
    raw = b'{"nonce":"' + b"e" * 64 + b'",' + raw[1:] + b"\n"
    with pytest.raises(ValueError):
        module.parse_request(raw)


def test_zero_census_rejects_every_extra_process_without_signalling(monkeypatch):
    from deerflow_ecs_fleet.worker import workspace_collector as module

    snapshot = {1: {**process(1, 0), "uid": 501}, 99: {**process(99, 0), "uid": 0}, 10: process(10, 1)}
    monkeypatch.setattr(module.os, "kill", lambda *args: pytest.fail("Independent census must never kill an unknown process"))
    with pytest.raises(ValueError, match="Unknown"):
        module.validate_zero_census(snapshot, helper_pid=99, workload_uid=501, helper_uid=0)


def test_zero_census_requires_birth_uid_isolation_and_original_workload_uid():
    from deerflow_ecs_fleet.worker import workspace_collector as module

    snapshot = {1: {**process(1, 0), "uid": 501}, 99: {**process(99, 0), "uid": 0}}
    module.validate_zero_census(snapshot, helper_pid=99, workload_uid=501, helper_uid=0)
    with pytest.raises(ValueError):
        module.validate_zero_census(snapshot, helper_pid=99, workload_uid=502, helper_uid=0)
    with pytest.raises(ValueError):
        module.validate_zero_census(snapshot, helper_pid=99, workload_uid=501, helper_uid=501)


@pytest.mark.parametrize("workload_user,helper_user", (("65534:65534", "0:0"), ("501:20", "0:0"), ("0:0", "65534:65534")))
def test_helper_uid_defensively_differs_from_every_frozen_workload_uid(workload_user, helper_user):
    from deerflow_ecs_fleet.worker import workspace_collector as module

    assert module.collector_helper_user(workload_user) == helper_user
