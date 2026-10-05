"""Original Popen adapter for host-registered Linux workspace tool supervisors."""

import hashlib
import json
import os
import select
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from . import workspace_supervisor
from .mutation_context import OwnershipRejected
from .workspace_boundary import original_workspace_tool_id


@dataclass(frozen=True)
class OriginalToolProcess:
    pid: int
    start_ticks: int
    role: str
    tool_execution_id: str
    start_nonce: str
    source_digest: str
    supervisor_pid: int | None = None
    supervisor_start_ticks: int | None = None


def supervisor_source_digest():
    return hashlib.sha256(Path(workspace_supervisor.__file__).read_bytes()).hexdigest()


class SupervisedCommand:
    """Keep the real supervisor Popen while returning original shell completion."""

    def __init__(self, *, args, env, stdout, stderr, register, pids_limit, retain, deadline):
        self.returncode = None
        self.deadline = deadline
        self.supervisor = None
        self._registered_supervisor = False
        self._registered_shell = False
        self.failure = None
        self._buffer = bytearray()
        self._closed = False
        self.pipe_drains = ()
        self.tool_execution_id = original_workspace_tool_id()
        self.start_nonce = uuid4().hex + uuid4().hex
        self.source_digest = supervisor_source_digest()
        control_read, self._control_write = os.pipe()
        self._receipt_read, receipt_write = os.pipe()
        try:
            self.process = subprocess.Popen(
                [sys.executable, "-I", "-S", str(Path(workspace_supervisor.__file__).resolve()), str(control_read), str(receipt_write)],
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
                pass_fds=(control_read, receipt_write),
            )
        except BaseException:
            os.close(self._control_write)
            os.close(self._receipt_read)
            raise
        finally:
            os.close(control_read)
            os.close(receipt_write)
        retain(self)
        try:
            self._send({"args": args, "env": env, "pids_limit": pids_limit})
            sup = self._receive(min(self.deadline, time.monotonic() + 30))
            self._verify_receipt(sup, "supervisor", self.process.pid)
            self.supervisor = OriginalToolProcess(self.process.pid, sup["start_ticks"], "supervisor", self.tool_execution_id, self.start_nonce, self.source_digest)
            register(self.supervisor, self.process)
            self._registered_supervisor = True
            self._send({"operation": "start"})
            shell = self._receive(min(self.deadline, time.monotonic() + 30))
            self._verify_receipt(shell, "shell", shell.get("pid"))
            if shell["ppid"] != self.process.pid:
                raise OwnershipRejected("Original shell does not belong to supervisor")
            self.shell = OriginalToolProcess(shell["pid"], shell["start_ticks"], "shell", self.tool_execution_id, self.start_nonce, self.source_digest, self.process.pid, sup["start_ticks"])
            register(self.shell, self.process)
            self._registered_shell = True
            self.pid = self.shell.pid
            self._send({"operation": "start"})
        except BaseException as error:
            # Retain the original real Popen/fds in the original host scope.
            # Final cleanup joins it on the original cumulative budget; no
            # constructor timeout may hide the first failure or drop ownership.
            self.failure = error
            if self.process.poll() is None:
                try:
                    self._send({"operation": "stop"})
                except OSError:
                    pass
            raise

    def _verify_receipt(self, value, event, pid):
        if value.get("event") != event or type(pid) is not int or pid <= 0:
            raise OwnershipRejected("Original supervisor process receipt rejected")
        fresh = workspace_supervisor.process_identity(pid)
        if any(value.get(key) != fresh[key] for key in ("pid", "start_ticks", "ppid")):
            raise OwnershipRejected("Original supervisor process start identity changed")

    def _send(self, value):
        data = json.dumps(value, separators=(",", ":")).encode() + b"\n"
        if len(data) > 1048576:
            raise ValueError("Private supervisor command exceeds bound")
        view = memoryview(data)
        while view:
            view = view[os.write(self._control_write, view) :]

    def _receive(self, deadline):
        while True:
            if b"\n" in self._buffer:
                line, _, remaining = self._buffer.partition(b"\n")
                self._buffer = bytearray(remaining)
                return json.loads(line)
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([self._receipt_read], [], [], remaining)[0]:
                raise subprocess.TimeoutExpired("original supervised shell", max(0, remaining))
            data = os.read(self._receipt_read, 4096)
            if not data:
                raise OwnershipRejected("Original supervisor exited without shell completion")
            self._buffer.extend(data)
            if len(self._buffer) > 8192:
                raise OwnershipRejected("Original supervisor receipt exceeds bound")

    def wait(self, timeout):
        if self.returncode is None:
            receipt = self._receive(time.monotonic() + timeout)
            if receipt.get("event") != "shell_exit" or type(receipt.get("returncode")) is not int:
                raise OwnershipRejected("Original shell completion receipt rejected")
            self.returncode = receipt["returncode"]
        return self.returncode

    def kill(self):
        workspace_supervisor.signal_owned(self.shell.pid, self.shell.start_ticks, signal.SIGKILL)

    def terminate_foreground(self):
        if self.process.poll() is None:
            self._send({"operation": "stop"})
        self.wait(timeout=10)

    def join(self, deadline):
        if self._closed:
            return
        self.process.wait(timeout=max(0, deadline - time.monotonic()))
        for thread in self.pipe_drains:
            thread.join(timeout=max(0, deadline - time.monotonic()))
            if thread.is_alive():
                raise TimeoutError("Original command pipe drain has not joined")
        os.close(self._control_write)
        os.close(self._receipt_read)
        self._closed = True

    def stop_and_join(self, deadline):
        if not self._closed and self.process.poll() is None:
            self._send({"operation": "stop"})
        self.join(deadline)
