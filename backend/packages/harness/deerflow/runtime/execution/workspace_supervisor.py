"""Installed Linux tool supervisor; private inherited pipes, no network/control token.

The sole non-filewriter operation is supervising the original shell. It becomes
its tool's subreaper before starting any user code, so double-fork/setsid writers
remain descendants of this registered identity after their shell exits.
"""

import ctypes
import json
import os
import select
import signal
import sys
import time

_census_limit = 4096


def process_identity(pid):
    """Read Linux proc identity without confusing spaces in comm with fields."""
    with open(f"/proc/{pid}/stat", encoding="utf-8") as stream:
        value = stream.read(8192)
    fields = value[value.rfind(")") + 2 :].split()
    return {"pid": pid, "start_ticks": int(fields[19]), "ppid": int(fields[1]), "pgid": int(fields[2]), "state": fields[0]}


def process_namespace(pid):
    return os.stat(f"/proc/{pid}/ns/pid").st_ino


def signal_owned(pid, start_ticks, sig, *, pid_namespace=None):
    """Bind the signal to a kernel handle before checking original identity."""
    try:
        fd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return False
    try:
        try:
            fresh = process_identity(pid)
            namespace = process_namespace(pid)
        except (FileNotFoundError, ProcessLookupError):
            return False
        expected_namespace = process_namespace(os.getpid()) if pid_namespace is None else pid_namespace
        if fresh["start_ticks"] != start_ticks or namespace != expected_namespace:
            raise ValueError("Original process identity changed before bound signal")
        try:
            signal.pidfd_send_signal(fd, sig, None, 0)
        except ProcessLookupError:
            return False
        return True
    finally:
        os.close(fd)


def _send(fd, value):
    data = json.dumps(value, separators=(",", ":")).encode() + b"\n"
    if len(data) > 4096:
        raise ValueError("Supervisor receipt exceeds bound")
    os.write(fd, data)


def _line(fd, deadline, limit=1048576):
    result = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([fd], [], [], remaining)[0]:
            raise TimeoutError("Supervisor start gate expired")
        value = os.read(fd, 1)
        if not value:
            raise RuntimeError("Original supervisor owner pipe closed")
        if value == b"\n":
            return json.loads(result)
        result.extend(value)
        if len(result) > limit:
            raise ValueError("Supervisor gate exceeds bound")


def _descendants(root, limit):
    identities = {}
    with os.scandir("/proc") as entries:
        scanned = 0
        for entry in entries:
            if entry.name.isdecimal():
                scanned += 1
                if scanned > limit:
                    raise RuntimeError("Contained process census exceeds frozen PID limit")
                try:
                    identity = process_identity(int(entry.name))
                except (FileNotFoundError, ProcessLookupError):
                    continue
                identities[identity["pid"]] = identity
    owned = {root}
    for _ in range(64):
        children = {pid for pid, identity in identities.items() if identity["ppid"] in owned}
        expanded = owned | children
        if expanded == owned:
            break
        owned = expanded
    else:
        raise RuntimeError("Contained process ancestry exceeds depth limit")
    return [identities[pid] for pid in owned - {root} if pid in identities]


def _stop_owned(root, limit):
    # Signals are selected by stable process identity inside this namespace,
    # never a command name or process group that a writer can escape.
    for identity in _descendants(root, limit):
        try:
            signal_owned(identity["pid"], identity["start_ticks"], signal.SIGKILL)
        except (ProcessLookupError, FileNotFoundError):
            pass


def _stop_and_reap(root, limit):
    """Keep this original ancestor alive until all adopted children are reaped.

    The host/Node execution deadline bounds its waiter. Failure to read/stop a
    child never permits this ancestor to exit and erase physical attribution;
    the original retained Popen/container owner must handle that failure.
    """
    while True:
        try:
            _stop_owned(root, limit)
        except (OSError, ValueError, IndexError, RuntimeError):
            # No successful quiescence receipt is emitted on this error path.
            pass
        try:
            while True:
                pid, _ = os.waitpid(-1, os.WNOHANG)
                if pid == 0:
                    break
        except ChildProcessError:
            return
        time.sleep(0.05)


def main(control_fd, receipt_fd):
    global _census_limit
    if sys.platform != "linux":
        raise RuntimeError("Workspace supervisor requires a Linux PID namespace")
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "Cannot establish tool child subreaper")
    deadline = time.monotonic() + 30
    payload = _line(control_fd, deadline)
    if not isinstance(payload, dict) or set(payload) != {"args", "env", "pids_limit"}:
        raise ValueError("Invalid private original command")
    limit = payload["pids_limit"]
    if type(limit) is not int or not 1 <= limit <= 4096:
        raise ValueError("Invalid frozen process census limit")
    _census_limit = limit
    _send(receipt_fd, {"event": "supervisor", **process_identity(os.getpid())})
    if _line(control_fd, deadline, 4096) != {"operation": "start"}:
        raise ValueError("Original owner did not register supervisor")
    # This single-threaded installed supervisor may safely fork. The child is
    # blocked before setsid/exec until the parent host fences its real identity.
    child_gate_read, child_gate_write = os.pipe()
    child = os.fork()
    if child == 0:
        os.close(child_gate_write)
        os.close(control_fd)
        os.close(receipt_fd)
        try:
            if os.read(child_gate_read, 1) != b"s":
                os._exit(125)
            os.close(child_gate_read)
            os.setsid()
            os.execvpe(payload["args"][0], payload["args"], payload["env"])
        except BaseException:
            os._exit(126)
    os.close(child_gate_read)
    _send(receipt_fd, {"event": "shell", **process_identity(child)})
    try:
        if _line(control_fd, deadline, 4096) != {"operation": "start"}:
            raise ValueError("Original owner did not register shell")
        os.write(child_gate_write, b"s")
    finally:
        os.close(child_gate_write)
    # Only the actual shell/writers retain capture pipes; the non-filewriter
    # supervisor must not extend the original output-drain lifetime.
    os.close(1)
    os.close(2)
    shell_reported = False
    stopping = False
    while True:
        try:
            while True:
                pid, status = os.waitpid(-1, os.WNOHANG)
                if pid == 0:
                    break
                if pid == child and not shell_reported:
                    _send(receipt_fd, {"event": "shell_exit", "returncode": os.waitstatus_to_exitcode(status)})
                    shell_reported = True
        except ChildProcessError:
            return
        if select.select([control_fd], [], [], 0.05)[0]:
            try:
                value = _line(control_fd, time.monotonic() + 1, 4096)
                if value != {"operation": "stop"}:
                    raise ValueError("Invalid original supervisor control")
            except (RuntimeError, TimeoutError, ValueError):
                stopping = True
            else:
                stopping = True
        if stopping:
            _stop_owned(os.getpid(), limit)


if __name__ == "__main__":
    try:
        main(int(sys.argv[1]), int(sys.argv[2]))
    except BaseException:
        if sys.platform == "linux":
            _stop_and_reap(os.getpid(), _census_limit)
        raise
