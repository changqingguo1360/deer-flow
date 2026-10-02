"""Agent Docker launch: one durable start, private bootstrap pipe and bounded cgroup."""

import asyncio
import hashlib
import json
import re
import time
from pathlib import Path

from ..config import NAME_PATTERN, ExecutionProfile
from .containers import DockerContainers, DockerError


class AgentContainers(DockerContainers):
    def __init__(self, *, state_dir, operator_config, provider="gateway", executable="docker"):
        super().__init__(state_dir=state_dir, executable=executable)
        if not re.fullmatch(NAME_PATTERN, provider):
            raise ValueError("Invalid installed provider name")
        self.operator_config = operator_config
        self.provider = provider
        self._attachments = {}
        self._drains = {}
        self.ready = {}
        self._original_claims = {}

    async def compatibility(self, image):
        # Trusted preflight reads the actual installed bundle in the approved
        # image. No credential or execution input is provided to this process.
        raw = await self.checked(
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--entrypoint",
            "python",
            image,
            "-I",
            "-S",
            "/opt/deerflow/libexec_bootstrap.py",
            "--compatibility",
            "--provider",
            self.provider,
            timeout=30,
        )
        from ..launch_spec import WorkerCompatibility

        return WorkerCompatibility.model_validate_json(raw).model_dump(mode="json")

    async def prepare_workspace(self, nas_root, claim, grant):
        root = Path(nas_root).resolve(strict=True)
        if self.state_dir.resolve().is_relative_to(root):
            raise ValueError("Private daemon recovery state must remain outside execution NAS")
        spec = grant["launch_spec"]
        pieces = (spec["user_id"], spec["thread_id"], "agents", spec["agent_task_id"], claim["attempt_id"])
        current = root
        for piece in pieces:
            if not re.fullmatch(NAME_PATTERN, piece):
                raise ValueError("Invalid Agent workspace identity")
            current = current / piece
            current.mkdir(mode=0o700, exist_ok=True)
            if current.is_symlink() or not current.resolve(strict=True).is_relative_to(root):
                raise ValueError("Agent workspace escapes NAS")
        from .agent_workspace import AgentWorkspaceSnapshots

        await asyncio.to_thread(AgentWorkspaceSnapshots(root, state_dir=self.state_dir / "prepared-workspaces", max_input_bytes=grant["input_limits"]["max_input_bytes"]).prepare, spec, current)
        return current

    def bind_claim(self, claim):
        # Original accepted token is retained only in the trusted daemon. The
        # runner receives its non-bearer stamp, never the node/attempt token.
        self._original_claims[claim["attempt_id"]] = hashlib.sha256(claim["token"].encode()).hexdigest()

    async def launch(self, grant, *, output_dir, input_dirs=None, deadline=None):
        if grant.get("kind") != "agent":
            return await super().launch(grant, output_dir=output_dir, input_dirs=input_dirs, deadline=deadline)
        if not grant.get("authorized") or min(grant.get("lease_seconds_remaining", 0), grant.get("execution_seconds_remaining", 0)) <= 0:
            raise ValueError("Valid Agent start authorization required")
        attempt = grant["attempt_id"]
        ref = grant["process_ref"]
        if not re.fullmatch(NAME_PATTERN, attempt) or ref != "fleet-" + attempt:
            raise ValueError("Invalid Agent process identity")
        profile = ExecutionProfile.model_validate(grant["execution_profile"])
        if profile.kind != "agent" or profile.runtime_digest != grant["launch_spec"]["runtime_digest"] or input_dirs:
            raise ValueError("Invalid frozen Agent profile")
        output = str(Path(output_dir).resolve(strict=True))
        if "," in output or self.state_dir.resolve().is_relative_to(Path(output)):
            raise ValueError("Unsafe Agent workspace")
        fingerprint = hashlib.sha256(json.dumps([grant["launch_spec"], grant["execution_profile"], output], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        observation = await self.inspect(ref)
        if observation is None:
            await self.checked(
                "create",
                "-i",
                "--pull=never",
                "--name",
                ref,
                "--label",
                "deerflow.fleet.attempt=" + attempt,
                "--label",
                "deerflow.fleet.node=" + grant["node_id"],
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
                "type=bind,src=" + output + ",dst=/workspace",
                "--env",
                "DEER_FLOW_HOME=/workspace/.deer-flow",
                "--workdir",
                "/workspace",
                "--entrypoint",
                "python",
                profile.image,
                "-I",
                "-S",
                "/opt/deerflow/libexec_bootstrap.py",
                "--provider",
                self.provider,
            )
            observation = await self.inspect(ref)
        labels = observation["Config"].get("Labels") or {}
        if labels.get("deerflow.fleet.launch") != fingerprint or labels.get("deerflow.fleet.attempt") != attempt:
            raise DockerError("Agent process reference conflicts with frozen authorization")
        if not await asyncio.to_thread(self.mark_start, ref, fingerprint):
            return observation
        stamp = self._original_claims.pop(attempt, None)
        if stamp is None:
            raise ValueError("Original accepted claim identity missing")
        identity = {key: grant[key] for key in ("node_id", "node_session_id", "agent_task_id", "generation", "attempt_id", "owner_worker_id")}
        identity["token_stamp"] = stamp
        payload = json.dumps({"bootstrap": {"schema_version": 1, "identity": identity, "operator_config": self.operator_config}, "grant": grant}, separators=(",", ":")).encode() + b"\n"
        if len(payload) > 1048576:
            raise ValueError("Private Agent bootstrap exceeds bound")
        proc = await asyncio.create_subprocess_exec(self.executable, "start", "-ai", ref, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        self._attachments[ref] = proc
        try:
            proc.stdin.write(payload)
            await proc.stdin.drain()
            proc.stdin.close()

            async def read_ready():
                total = 0
                while True:
                    line = await proc.stdout.readline()
                    total += len(line)
                    if not line or total > 65536:
                        raise DockerError("Agent bootstrap did not acknowledge readiness")
                    try:
                        ready = json.loads(line)
                    except (ValueError, UnicodeError):
                        continue
                    if ready.get("ready") is True and ready.get("attempt_id") == attempt and isinstance(ready.get("pid"), int):
                        self.ready[ref] = ready
                        return

            timeout = min(30, (deadline - time.monotonic()) if deadline is not None else 30)
            if timeout <= 0:
                raise DockerError("Agent start lease elapsed")
            await asyncio.wait_for(read_ready(), timeout)

            async def drain():
                while await proc.stdout.read(65536):
                    pass
                await proc.wait()

            self._drains[ref] = asyncio.create_task(drain())
            return await self.inspect(ref)
        except BaseException:
            await self.stop(ref)
            if proc.returncode is None:
                proc.kill()
            await proc.wait()
            raise

    async def stop(self, ref):
        proof = await super().stop(ref)
        proc = self._attachments.pop(ref, None)
        if proc is not None and proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), 5)
            except TimeoutError:
                proc.kill()
                await proc.wait()
        drain = self._drains.pop(ref, None)
        if drain is not None:
            await asyncio.gather(drain, return_exceptions=True)
        return proof
