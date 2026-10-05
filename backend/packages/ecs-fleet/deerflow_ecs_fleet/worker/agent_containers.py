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
        self._workspace_tokens = {}
        self._workspace_execs = {}
        self._workspace_namespaces = {}
        self._workspace_readers = {}
        self._workspace_container_ids = {}

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

        compatibility = WorkerCompatibility.model_validate_json(raw)
        if compatibility.workspace_contract_version != 1:
            raise ValueError("Installed workspace contracts required for Agent preflight")
        return compatibility.model_dump(mode="json")

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

        snapshots = AgentWorkspaceSnapshots(root, state_dir=self.state_dir / "prepared-workspaces", max_input_bytes=grant["input_limits"]["max_input_bytes"])
        if grant.get("accepted_workspace") is not None:
            preparation = asyncio.to_thread(snapshots.prepare_accepted, spec, current, grant["accepted_workspace"], nas_identity=grant["nas_identity"], max_output_bytes=grant["execution_profile"]["max_output_bytes"])
        else:
            preparation = asyncio.to_thread(snapshots.prepare, spec, current)
        # This private task owns the physical native writer. Cancellation of
        # execute must not release its attempt, daemon flock or client while
        # restore/fsync/control-marker writes are still running.
        writer = asyncio.create_task(preparation)
        cancellation = None
        while not writer.done():
            try:
                await asyncio.shield(writer)
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
            except BaseException:
                break  # retrieve the original writer failure below
        try:
            writer.result()
        except BaseException as error:
            if cancellation is not None:
                raise BaseExceptionGroup("Cancelled Agent preparation and native writer failure", [cancellation, error]) from None
            raise
        if cancellation is not None:
            raise cancellation
        return current

    def bind_claim(self, claim):
        # Original accepted token is retained only in the trusted daemon. The
        # runner receives its non-bearer stamp, never the node/attempt token.
        stamp = hashlib.sha256(claim["token"].encode()).hexdigest()
        self._original_claims[claim["attempt_id"]] = stamp
        self._workspace_tokens[claim["attempt_id"]] = stamp

    def _workspace_observation(self, observation, grant, output):
        profile = ExecutionProfile.model_validate(grant["execution_profile"])
        if profile.kind != "agent" or profile.runtime_digest != grant["launch_spec"]["runtime_digest"]:
            raise DockerError("Original frozen Agent workspace profile required")
        if observation is None or not re.fullmatch(r"[a-f0-9]{64}", observation.get("Id", "")):
            raise DockerError("Immutable Agent container identity unavailable")
        state, config, host = observation["State"], observation["Config"], observation["HostConfig"]
        fingerprint = hashlib.sha256(json.dumps([grant["launch_spec"], grant["execution_profile"], str(output)], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        labels = config.get("Labels") or {}
        if (
            state.get("Running") is not True
            or state.get("Paused") is not False
            or not state.get("StartedAt")
            or observation["Image"] != profile.image.rsplit("@", 1)[-1]
            or config.get("Image") != profile.image
            or config.get("User") != profile.user
            or config.get("Entrypoint") != ["python"]
            or config.get("Cmd") != ["-I", "-S", "/opt/deerflow/libexec_bootstrap.py", "--provider", self.provider]
            or config.get("WorkingDir") != "/workspace"
            or labels.get("deerflow.fleet.attempt") != grant["attempt_id"]
            or labels.get("deerflow.fleet.node") != grant["node_id"]
            or labels.get("deerflow.fleet.launch") != fingerprint
            or host.get("Privileged") is not False
            or host.get("CapAdd") not in (None, [])
            or host.get("PidMode") != ""
            or host.get("UsernsMode") != ""
            or host.get("CgroupnsMode") != "private"
            or host.get("CgroupParent") != ""
            or host.get("ReadonlyRootfs") is not True
            or host.get("PidsLimit") != profile.pids_limit
            or host.get("Memory") != profile.memory_mib * 1048576
            or host.get("NanoCpus") != profile.cpu_millis * 1000000
            or host.get("NetworkMode") != profile.network
            or set(host.get("CapDrop") or ()) != {"ALL"}
            or host.get("SecurityOpt") != ["no-new-privileges"]
            or (host.get("RestartPolicy") or {}).get("Name") != "no"
        ):
            raise DockerError("Agent container differs from original immutable authorization")
        mounts = observation.get("Mounts") or []
        if len(mounts) != 1 or any(mount.get("Type") != "bind" or mount.get("Source") != str(output) or mount.get("Destination") != "/workspace" or mount.get("RW") is not True for mount in mounts):
            raise DockerError("Agent container workspace mount conflicts")
        return observation["Id"], state["StartedAt"], observation["Image"], fingerprint

    async def quiesce_workspace(self, grant, *, output_dir, request, deadline):
        """Trusted Node exec census; never accepts a point or runner assertion."""
        from deerflow.runtime.execution.workspace_process import supervisor_source_digest

        from ..agent_workspace import WorkspaceBoundaryIdentity
        from ..launch_spec import LaunchSpec
        from . import workspace_collector

        values = dict(request["identity"])
        values["presented_paths"] = tuple(values["presented_paths"])
        identity = WorkspaceBoundaryIdentity(**values)
        spec = LaunchSpec.model_validate(grant["launch_spec"])
        if (
            identity.request_digest != request["request_digest"]
            or identity.launch_spec_digest != spec.payload_digest()
            or any(getattr(identity, name) != grant[name] for name in ("node_id", "node_session_id", "agent_task_id", "run_id", "generation", "attempt_id", "owner_worker_id", "process_ref"))
            or (identity.user_id, identity.thread_id) != (spec.user_id, spec.thread_id)
            or identity.token_stamp != self._workspace_tokens.get(identity.attempt_id)
            or request["state"] not in {"sealing", "prepared"}
        ):
            raise DockerError("Workspace census original execution conflicts")
        ref = grant["process_ref"]
        output = Path(output_dir).resolve(strict=True)
        original = self._workspace_observation(await self.inspect(ref), grant, output)
        existing = self._workspace_execs.get(ref)
        if existing is not None and existing.returncode is None:
            raise DockerError("Original workspace collector CLI remains active")
        timeout = min(30, deadline - time.monotonic())
        if timeout <= 0:
            raise DockerError("Original workspace census deadline elapsed")
        payload = {
            "nonce": request["nonce"],
            "request_digest": identity.request_digest,
            "identity": request["identity"],
            "barrier_epoch": request["barrier_epoch"],
            "processes": request["processes"],
            "pids_limit": grant["execution_profile"]["pids_limit"],
            "supervisor_digest": supervisor_source_digest(),
            "timeout_seconds": timeout,
            "workload_user": grant["execution_profile"]["user"],
            "helper_user": workspace_collector.collector_helper_user(grant["execution_profile"]["user"]),
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
        workspace_collector.parse_request(raw)
        proc = await asyncio.create_subprocess_exec(
            self.executable,
            "exec",
            "-i",
            "--user",
            payload["helper_user"],
            original[0],
            "python",
            "-I",
            "-S",
            workspace_collector.COLLECTOR_PATH,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._workspace_execs[ref] = proc

        async def bounded_read(stream):
            result = bytearray()
            while True:
                chunk = await stream.read(min(4096, 4097 - len(result)))
                if not chunk:
                    return bytes(result)
                result.extend(chunk)
                if len(result) > 4096:
                    raise DockerError("Trusted workspace collector receipt exceeds bound")

        readers = (asyncio.create_task(bounded_read(proc.stdout)), asyncio.create_task(bounded_read(proc.stderr)), asyncio.create_task(proc.wait()))
        self._workspace_readers[ref] = readers
        self._workspace_container_ids[ref] = original[0]
        try:
            async with asyncio.timeout(max(0, deadline - time.monotonic())):
                proc.stdin.write(raw)
                await proc.stdin.drain()
                proc.stdin.close()
                stdout, stderr, code = await asyncio.gather(*readers)
            if code != 0 or stderr or not stdout.endswith(b"\n") or stdout.count(b"\n") != 1:
                raise DockerError("Trusted workspace collector did not complete cleanly")

            def strict_fields(pairs):
                values = {}
                for key, value in pairs:
                    if key in values:
                        raise DockerError("Duplicate trusted workspace receipt field")
                    values[key] = value
                return values

            receipt = json.loads(stdout, object_pairs_hook=strict_fields)
            if set(receipt) != {"nonce", "request_digest", "barrier_epoch", "collector", "runner", "remaining_pids", "collector_digest", "cgroup_digest"}:
                raise DockerError("Trusted workspace collector receipt fields conflict")
            own, runner = receipt["collector"], receipt["runner"]
            if (
                receipt["nonce"] != request["nonce"]
                or receipt["request_digest"] != identity.request_digest
                or receipt["barrier_epoch"] != request["barrier_epoch"]
                or receipt["collector_digest"] != hashlib.sha256(Path(workspace_collector.__file__).read_bytes()).hexdigest()
                or type(own["pid"]) is not int
                or own["pid"] <= 1
                or type(own["start_ticks"]) is not int
                or own["start_ticks"] <= 0
                or runner["pid"] != 1
                or type(runner["start_ticks"]) is not int
                or runner["start_ticks"] <= 0
                or type(own["pid_namespace"]) is not int
                or own["pid_namespace"] <= 0
                or own.get("uid") != int(payload["helper_user"].split(":", 1)[0])
                or runner.get("uid") != int(payload["workload_user"].split(":", 1)[0])
                or not re.fullmatch(r"[a-f0-9]{64}", receipt["cgroup_digest"])
                or receipt["remaining_pids"] != [1, own["pid"]]
            ):
                raise DockerError("Trusted workspace collector identity conflicts")
            namespace = self._workspace_namespaces.get(original[0], own["pid_namespace"])
            if namespace != own["pid_namespace"] or self._workspace_observation(await self.inspect(original[0]), grant, output) != original:
                raise DockerError("Original Agent container changed during census")
            if time.monotonic() >= deadline:
                raise DockerError("Original workspace census deadline elapsed")
            self._workspace_namespaces[original[0]] = namespace
            return {"container_id": original[0], "started_at": original[1], "image": original[2], "launch_fingerprint": original[3], "receipt": receipt, "cli_pid": proc.pid, "cli_exit_code": proc.returncode}
        except BaseException:
            if proc.returncode is None:
                proc.kill()
            await asyncio.shield(proc.wait())
            await asyncio.shield(asyncio.gather(*readers, return_exceptions=True))
            raise
        finally:
            if all(task.done() for task in readers):
                self._workspace_readers.pop(ref, None)

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
        census = self._workspace_execs.get(ref)
        if census is not None:
            if census.returncode is None:
                census.kill()
            await asyncio.shield(census.wait())
            readers = self._workspace_readers.get(ref, ())
            if readers:
                await asyncio.shield(asyncio.gather(*readers, return_exceptions=True))
            self._workspace_readers.pop(ref, None)
            self._workspace_execs.pop(ref, None)
        if proof:
            self._workspace_tokens.pop(ref.removeprefix("fleet-"), None)
            self._original_claims.pop(ref.removeprefix("fleet-"), None)
            self._workspace_namespaces.pop(self._workspace_container_ids.pop(ref, None), None)
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
