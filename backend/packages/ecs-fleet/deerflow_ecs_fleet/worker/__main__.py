"""Outbound worker entry: private file credentials and persistent recovery state."""

import argparse
import asyncio
import fcntl
import json
import os
import signal
import stat
import sys
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..config import NAME_PATTERN
from ..workspace import NASWorkspace
from .client import NodeClient
from .containers import DockerContainers
from .daemon import NodeDaemon
from .journal import AttemptJournal


def read_file(path: Path, *, private=False, limit=65536):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
            raise ValueError("Unsafe settings file")
        if private and (info.st_mode & 0o077 or info.st_uid != os.getuid()):
            raise PermissionError("Private file required")
        data = stream.read(limit + 1)
        if len(data) > limit:
            raise ValueError("Settings file exceeds limit")
        return data


class WorkerSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    gateway_url: str
    credential_file: Path
    state_dir: Path
    nas_root: Path
    nas_identity: str = Field(pattern=NAME_PATTERN)
    timeout_seconds: float = Field(default=10, ge=0.1, le=60)
    renew_seconds: float = Field(default=10, ge=0.1, le=30)
    safety_margin_seconds: float = Field(default=5, ge=0.1, le=15)
    poll_seconds: float = Field(default=0.25, ge=0.01, le=10)
    max_parallel: int = Field(default=1, ge=1, le=64)

    @model_validator(mode="after")
    def paths(self):
        for path in (self.credential_file, self.state_dir, self.nas_root):
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError("Absolute local paths required")
        nas = self.nas_root.resolve()
        state = self.state_dir.resolve()
        credential = self.credential_file.resolve()
        if state.is_relative_to(nas) or nas.is_relative_to(state) or credential.is_relative_to(nas):
            raise ValueError("Private state and credentials must be outside NAS")
        return self


async def run_worker(settings: WorkerSettings):
    credential = read_file(settings.credential_file, private=True, limit=256).decode().strip()
    workspace = NASWorkspace(settings.nas_root, identity=settings.nas_identity)
    await asyncio.to_thread(workspace.validate_root)
    client = NodeClient(gateway_url=settings.gateway_url, credential=credential, timeout_seconds=settings.timeout_seconds)
    lock = None
    installed = []
    try:
        journal = AttemptJournal(settings.state_dir)
        journal.directory()
        lock = os.open(settings.state_dir / ".daemon.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
            installed.append(sig)
        daemon = NodeDaemon(
            client=client,
            containers=DockerContainers(state_dir=settings.state_dir),
            state_dir=settings.state_dir,
            workspace=workspace,
            renew_seconds=settings.renew_seconds,
            safety_margin_seconds=settings.safety_margin_seconds,
            poll_seconds=settings.poll_seconds,
        )
        await daemon.run(stop=stop, max_parallel=settings.max_parallel)
        records = await asyncio.to_thread(journal.records)
        if any(
            not row.get("reported") or (row.get("completion_manifest") is not None and not row.get("completion_reported")) or (row.get("server_state") == "running" and row.get("stop_reason") == "exit" and row.get("exit_code") == 0)
            for row in records
        ):
            raise RuntimeError("Worker shutdown requires recovery of unacknowledged stop or completion")
    finally:
        for sig in installed:
            asyncio.get_running_loop().remove_signal_handler(sig)
        if lock is not None:
            os.close(lock)
        await client.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Outbound Fleet worker; credentials are read only from a private file.")
    parser.add_argument("--settings", type=Path, required=True, help="Path to a bounded JSON worker settings file")
    args = parser.parse_args(argv)
    try:
        settings = WorkerSettings.model_validate(json.loads(read_file(args.settings)))
        asyncio.run(run_worker(settings))
    except KeyboardInterrupt:
        return 1
    except Exception:
        # Validation, HTTP and Docker exceptions may contain credentials,
        # deployment paths or launch data. Never stringify these here.
        print("Worker startup or recovery failed; retain private state and reconcile before restarting.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
