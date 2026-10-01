"""Host-only Docker control with durable, one-shot start intent per attempt."""

import asyncio
import hashlib
import json
import os
import re
import stat
from pathlib import Path

from ..config import NAME_PATTERN, ExecutionProfile


class DockerError(RuntimeError):
    pass


class DockerContainers:
    def __init__(self, *, state_dir: Path, executable: str = "docker"):
        self.state_dir = Path(state_dir)
        self.executable = executable

    async def command(self, *args, timeout=15):
        proc = await asyncio.create_subprocess_exec(self.executable, *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except BaseException:
            if proc.returncode is None:
                proc.kill()
                await proc.wait()
            raise
        return proc.returncode, stdout.decode(), stderr.decode()

    async def checked(self, *args):
        code, stdout, stderr = await self.command(*args)
        if code:
            raise DockerError(stderr.strip() or "Docker command failed")
        return stdout

    async def inspect(self, ref):
        code, stdout, stderr = await self.command("inspect", ref)
        if code:
            if "no such object" in stderr.lower() or "no such container" in stderr.lower():
                return None
            raise DockerError(stderr.strip() or "Docker inspect failed")
        rows = json.loads(stdout)
        if len(rows) != 1:
            raise DockerError("Unexpected Docker inspection result")
        return rows[0]

    def mark_start(self, ref, fingerprint):
        self.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.state_dir.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
            raise PermissionError("Worker state directory must be private and owned by the daemon")
        path = self.state_dir / (ref + ".start")
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            return False
        try:
            os.write(fd, fingerprint.encode())
            os.fsync(fd)
        finally:
            os.close(fd)
        fd = os.open(self.state_dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return True

    async def launch(self, grant, *, output_dir: Path):
        if not grant.get("authorized") or grant.get("lease_seconds_remaining", 0) <= 0 or grant.get("execution_seconds_remaining", 0) <= 0:
            raise ValueError("Valid start authorization required")
        attempt_id = grant["attempt_id"]
        if not re.fullmatch(NAME_PATTERN, attempt_id) or grant["process_ref"] != "fleet-" + attempt_id:
            raise ValueError("Invalid authorized process identity")
        ref = grant["process_ref"]
        profile = ExecutionProfile.model_validate(grant["launch_spec"]["profile"])
        output = str(Path(output_dir).resolve(strict=True))
        if "," in output:
            raise ValueError("Unsupported mount path")
        argv = grant["launch_spec"]["spec"]["argv"]
        if not argv or any(not isinstance(value, str) or "\0" in value for value in argv):
            raise ValueError("Invalid execution argv")
        fingerprint = hashlib.sha256(json.dumps([grant["launch_spec"], output], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        observation = await self.inspect(ref)
        if observation is None:
            args = [
                "create",
                "--pull=never",
                "--name",
                ref,
                "--label",
                "deerflow.fleet.attempt=" + attempt_id,
                "--label",
                "deerflow.fleet.launch=" + fingerprint,
                "--restart=no",
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--pids-limit",
                str(profile.pids_limit),
                "--network",
                profile.network,
                "--user",
                profile.user,
                "--cpus",
                str(profile.cpu_millis / 1000),
                "--memory",
                str(profile.memory_mib) + "m",
                "--log-driver=json-file",
                "--log-opt",
                "max-size=" + str(profile.max_log_bytes),
                "--log-opt",
                "max-file=1",
                "--tmpfs",
                "/tmp:rw,noexec,nosuid,size=64m",
                "--mount",
                "type=bind,src=" + output + ",dst=/output",
                "--workdir",
                "/output",
                "--entrypoint",
                "",
                profile.image,
                *argv,
            ]
            code, _, stderr = await self.command(*args)
            if code and "already in use" not in stderr:
                raise DockerError(stderr.strip() or "Docker create failed")
            observation = await self.inspect(ref)
        labels = observation["Config"].get("Labels") or {}
        if labels.get("deerflow.fleet.attempt") != attempt_id or labels.get("deerflow.fleet.launch") != fingerprint:
            raise DockerError("Existing container belongs to another launch")
        if observation["State"]["Status"] == "created":
            # A crash between fsync and docker start is uncertain, never an
            # excuse to issue another start. Reconciliation must stop/inspect it.
            if await asyncio.to_thread(self.mark_start, ref, fingerprint):
                await self.checked("start", ref)
                observation = await self.inspect(ref)
        return observation

    async def stop(self, ref) -> bool:
        if not re.fullmatch(r"fleet-[a-zA-Z0-9_.-]+", ref):
            raise ValueError("Invalid Fleet process reference")
        observation = await self.inspect(ref)
        if observation is None:
            return True
        labels = observation["Config"].get("Labels") or {}
        if labels.get("deerflow.fleet.attempt") != ref.removeprefix("fleet-"):
            raise DockerError("Refusing to stop a container not managed by this Fleet attempt")
        if observation["State"]["Running"] or observation["State"].get("Paused"):
            code, _, stderr = await self.command("kill", ref)
            if code:
                observation = await self.inspect(ref)
                if observation is not None and observation["State"]["Running"]:
                    raise DockerError(stderr.strip() or "Docker kill failed")
        observation = await self.inspect(ref)
        return observation is None or not observation["State"]["Running"]
