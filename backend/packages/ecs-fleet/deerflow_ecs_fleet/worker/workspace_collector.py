"""Fixed installed, stdlib-only Linux process census for trusted Node exec."""

import ctypes
import hashlib
import json
import os
import signal
import stat
import sys
import time
from pathlib import Path

MAX_MESSAGE_BYTES = 2 * 1024 * 1024
MAX_DEPTH = 64
COLLECTOR_PATH = "/opt/deerflow/libexec_workspace_collector.py"


def installed_collector_bytes():
    """The fixed readonly entrypoint must equal the actual installed wheel code."""
    fd = os.open(COLLECTOR_PATH, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > MAX_MESSAGE_BYTES:
            raise ValueError("Invalid fixed installed collector entrypoint")
        content = bytearray()
        while True:
            chunk = os.read(fd, min(65536, MAX_MESSAGE_BYTES + 1 - len(content)))
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > MAX_MESSAGE_BYTES:
                raise ValueError("Installed collector source exceeds bound")
        after = os.fstat(fd)
        if (before.st_ino, before.st_dev, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_ino, after.st_dev, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError("Installed collector changed during validation")
        if bytes(content) != Path(__file__).read_bytes():
            raise ValueError("Fixed collector differs from actual packaged code")
        return bytes(content)
    finally:
        os.close(fd)


def kernel_capability():
    """Verify kernel ABI without changing the original runner's prctl flags."""
    if not sys.platform.startswith("linux") or not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise ValueError("Linux workspace process kernel contract unavailable")
    kernel = ctypes.CDLL(None, use_errno=True)
    dumpable = kernel.prctl(3, 0, 0, 0, 0)
    subreaper = ctypes.c_int()
    if dumpable not in {0, 1} or kernel.prctl(37, ctypes.byref(subreaper), 0, 0, 0) != 0 or subreaper.value not in {0, 1}:
        raise ValueError("Original Linux process flags unavailable")
    try:
        if kernel.prctl(36, 1, 0, 0, 0) != 0 or kernel.prctl(4, 0, 0, 0, 0) != 0:
            raise ValueError("Linux workspace subreaper/hardening unavailable")
        fd = os.pidfd_open(os.getpid(), 0)
        try:
            signal.pidfd_send_signal(fd, 0, None, 0)
        finally:
            os.close(fd)
    finally:
        # Both restores are attempted, even if one fails. The collector's own
        # main has a separate permanent nondumpable contract for private input.
        restored_subreaper = kernel.prctl(36, subreaper.value, 0, 0, 0)
        restored_dumpable = kernel.prctl(4, dumpable, 0, 0, 0)
        if restored_subreaper != 0 or restored_dumpable != 0:
            raise ValueError("Original Linux process flags could not be restored")


def process_identity(pid):
    raw = Path("/proc", str(pid), "stat").read_text()
    tail = raw[raw.rfind(")") + 2 :].split()
    if len(tail) < 20 or int(raw.split(" ", 1)[0]) != pid:
        raise ValueError("Incomplete process identity")
    return {"pid": pid, "ppid": int(tail[1]), "start_ticks": int(tail[19]), "state": tail[0]}


def process_census(limit):
    if type(limit) is not int or not 1 <= limit <= 4096:
        raise ValueError("Frozen process bound required")
    snapshot = {}
    scanned = 0
    with os.scandir("/proc") as entries:
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            scanned += 1
            if scanned > limit:
                raise ValueError("Process census exceeds frozen bound")
            try:
                record = process_identity(int(entry.name))
            except (FileNotFoundError, ProcessLookupError) as error:
                raise ValueError("Incomplete contained process census") from error
            snapshot[record["pid"]] = record
    return snapshot


def collector_helper_user(workload_user):
    values = workload_user.split(":")
    if len(values) > 2 or not values[0].isdecimal() or (len(values) == 2 and not values[1].isdecimal()):
        raise ValueError("Frozen workload user must be numeric")
    return "65534:65534" if int(values[0]) == 0 else "0:0"


def process_uid(pid):
    with open(f"/proc/{pid}/status", encoding="ascii") as stream:
        raw = stream.read(8193)
    if len(raw) > 8192:
        raise ValueError("Process UID status exceeds bound")
    values = [line.split()[1:] for line in raw.splitlines() if line.startswith("Uid:")]
    if len(values) != 1 or len(values[0]) != 4 or any(not value.isdecimal() for value in values[0]) or len(set(values[0])) != 1:
        raise ValueError("Original workload process UID unavailable")
    return int(values[0][0])


def validate_zero_census(snapshot, *, helper_pid, workload_uid, helper_uid):
    if type(workload_uid) is not int or workload_uid < 0 or type(helper_uid) is not int or helper_uid < 0 or workload_uid == helper_uid:
        raise ValueError("Trusted helper birth UID must differ from workload")
    if type(helper_pid) is not int or helper_pid <= 1 or set(snapshot) != {1, helper_pid}:
        raise ValueError("Unknown process remains in original container")
    for pid, uid in ((1, workload_uid), (helper_pid, helper_uid)):
        record = snapshot[pid]
        if record["pid"] != pid or type(record["start_ticks"]) is not int or record["start_ticks"] <= 0 or record["uid"] != uid or record["state"] in {"Z", "X", "x"}:
            raise ValueError("Original process identity/UID conflicts")


def parse_request(raw):
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate collector request key")
            result[key] = value
        return result

    if len(raw) > MAX_MESSAGE_BYTES or not raw.endswith(b"\n"):
        raise ValueError("Bounded collector request required")
    request = json.loads(raw, object_pairs_hook=unique_pairs)
    if not isinstance(request, dict) or set(request) != {"nonce", "request_digest", "identity", "barrier_epoch", "processes", "pids_limit", "supervisor_digest", "timeout_seconds", "workload_user", "helper_user"}:
        raise ValueError("Collector request fields conflict")
    for name in ("nonce", "request_digest", "supervisor_digest"):
        value = request[name]
        if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("Collector request digest invalid")
    identity = request["identity"]
    fields = {
        "request_id",
        "user_id",
        "thread_id",
        "run_id",
        "agent_task_id",
        "generation",
        "attempt_id",
        "launch_spec_digest",
        "node_id",
        "node_session_id",
        "owner_worker_id",
        "token_stamp",
        "checkpoint_id",
        "kind",
        "publication_key",
        "presented_paths",
        "source_workspace_version",
        "checkpoint_ns",
        "process_ref",
        "desired_core_status",
        "desired_task_status",
        "desired_placement_status",
        "error",
        "stop_reason",
    }
    if not isinstance(identity, dict) or set(identity) != fields or hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest() != request["request_digest"]:
        raise ValueError("Full original collector identity conflicts")
    if type(request["pids_limit"]) is not int or not 1 <= request["pids_limit"] <= 4096:
        raise ValueError("Frozen collector process bound invalid")
    if type(request["barrier_epoch"]) is not int or request["barrier_epoch"] <= 0 or not isinstance(request["processes"], list) or len(request["processes"]) > request["pids_limit"]:
        raise ValueError("Collector request epoch/registry bound invalid")
    process_fields = {"pid", "start_ticks", "role", "tool_execution_id", "start_nonce", "source_digest", "supervisor_pid", "supervisor_start_ticks"}
    seen = set()
    for record in request["processes"]:
        if not isinstance(record, dict) or set(record) != process_fields or record["role"] not in {"shell", "supervisor"}:
            raise ValueError("Original collector process record invalid")
        if any(type(record[key]) is not int or record[key] <= 0 for key in ("pid", "start_ticks")) or record["pid"] in seen:
            raise ValueError("Original collector process identity invalid")
        seen.add(record["pid"])
        for key in ("tool_execution_id", "start_nonce", "source_digest"):
            if not isinstance(record[key], str) or len(record[key]) != 64 or any(char not in "0123456789abcdef" for char in record[key]):
                raise ValueError("Original collector process evidence invalid")
        parent = (record["supervisor_pid"], record["supervisor_start_ticks"])
        if (record["role"] == "supervisor" and parent != (None, None)) or (record["role"] == "shell" and any(type(value) is not int or value <= 0 for value in parent)):
            raise ValueError("Original collector process parent invalid")
    if not isinstance(request["workload_user"], str) or request["helper_user"] != collector_helper_user(request["workload_user"]):
        raise ValueError("Original helper birth UID contract conflicts")
    if request["processes"]:
        raise ValueError("Original producer has not positively settled registered processes")
    timeout = request["timeout_seconds"]
    if type(timeout) not in (int, float) or not 0 < timeout <= 30:
        raise ValueError("Bounded original execution wait required")
    return request


def main():
    # Reject before accepting private input unless Linux prevents same-uid
    # processes from reading our private descriptors or ptracing this exec.
    if not sys.platform.startswith("linux") or ctypes.CDLL(None, use_errno=True).prctl(4, 0, 0, 0, 0) != 0:
        raise ValueError("Trusted collector hardening unavailable")
    raw = sys.stdin.buffer.readline(MAX_MESSAGE_BYTES + 1)
    if len(raw) > MAX_MESSAGE_BYTES or not raw.endswith(b"\n") or sys.stdin.buffer.read(1):
        raise ValueError("Bounded single collector request required")
    request = parse_request(raw)
    timeout = request["timeout_seconds"]
    deadline = time.monotonic() + timeout
    workload_uid = int(request["workload_user"].split(":", 1)[0])
    helper_uid = int(request["helper_user"].split(":", 1)[0])
    if os.getuid() != helper_uid or os.geteuid() != helper_uid or ctypes.CDLL(None, use_errno=True).prctl(39, 0, 0, 0, 0) != 1:
        raise ValueError("Actual trusted helper UID/no-new-privileges conflicts")
    snapshot = process_census(request["pids_limit"])
    snapshot[1]["uid"] = process_uid(1)
    snapshot[os.getpid()]["uid"] = process_uid(os.getpid())
    validate_zero_census(snapshot, helper_pid=os.getpid(), workload_uid=workload_uid, helper_uid=helper_uid)
    own = dict(snapshot[os.getpid()])
    own["pid_namespace"] = Path("/proc/self/ns/pid").stat().st_ino
    with open("/proc/self/cgroup", "rb") as stream:
        own_cgroup = stream.read(4097)
    with open("/proc/1/cgroup", "rb") as stream:
        runner_cgroup = stream.read(4097)
    if len(own_cgroup) > 4096 or own_cgroup != runner_cgroup:
        raise ValueError("Original container cgroup conflicts")
    # A disappearance during inventory is incomplete, never success. Repeat a
    # full bounded inventory after UID/cgroup reads before issuing one receipt.
    fresh = process_census(request["pids_limit"])
    fresh[1]["uid"] = process_uid(1)
    fresh[os.getpid()]["uid"] = process_uid(os.getpid())
    validate_zero_census(fresh, helper_pid=os.getpid(), workload_uid=workload_uid, helper_uid=helper_uid)
    for pid in (1, os.getpid()):
        if any(fresh[pid][name] != snapshot[pid][name] for name in ("pid", "ppid", "start_ticks", "uid")):
            raise ValueError("Original process changed during independent inventory")
    if time.monotonic() >= deadline:
        raise TimeoutError("Original census execution deadline elapsed")
    receipt = {
        "nonce": request["nonce"],
        "request_digest": request["request_digest"],
        "barrier_epoch": request["barrier_epoch"],
        "collector": own,
        "runner": fresh[1],
        "remaining_pids": sorted(fresh),
        "cgroup_digest": hashlib.sha256(own_cgroup).hexdigest(),
        "collector_digest": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    sys.stdout.buffer.write(json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode() + b"\n")
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()
