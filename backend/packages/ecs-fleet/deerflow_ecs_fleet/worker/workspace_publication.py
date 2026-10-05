"""Trusted Node staging on the original live attempt, never point acceptance."""

import asyncio
import hashlib
import os
import time
from copy import deepcopy
from pathlib import Path
from secrets import token_hex

from ..agent_workspace import AgentWorkspaceVersions, WorkspaceBoundaryIdentity
from ..artifacts import OPEN_DIRECTORY
from ..launch_spec import LaunchSpec


class WorkspacePublicationStopped(RuntimeError):
    pass


class AgentWorkspacePublication:
    def __init__(self, *, client, containers, nas, journal):
        self.client = client
        self.containers = containers
        self.nas = nas
        self.journal = journal
        # Cancellation of an awaiter cannot terminate the physical copy. These
        # original tasks remain owned even after their deadline has elapsed.
        self._copies = {}
        self._save_lock = asyncio.Lock()
        self._saves = set()
        self._closing = False
        self._join_task = None

    @property
    def pending_saves(self):
        return tuple(task for task in self._saves if not task.done())

    async def save_record(self, record):
        if self._closing:
            raise WorkspacePublicationStopped("Original publication writer admission is closed")
        snapshot = deepcopy(record)

        async def actual_save():
            # This retained task owns the lock through physical native write.
            # Cancelled stage awaiters cannot release it or race a stopped save.
            async with self._save_lock:
                await asyncio.to_thread(self.journal.save, snapshot)

        task = asyncio.create_task(actual_save())
        self._saves.add(task)

        def completed(actual):
            self._saves.discard(actual)
            if not actual.cancelled():
                actual.exception()

        task.add_done_callback(completed)
        await asyncio.shield(task)

    @property
    def pending_copies(self):
        return tuple(task for task in self._copies.values() if not task.done())

    async def join_writers(self):
        # Close admission only after the original daemon stopped/joined its
        # execute tasks. This resource join grants no publication authorization.
        self._closing = True
        if self._join_task is None:

            async def actual_join():
                while self.pending_copies or self.pending_saves:
                    await asyncio.gather(*self.pending_copies, *self.pending_saves, return_exceptions=True)

            self._join_task = asyncio.create_task(actual_join())
        cancelled = False
        while not self._join_task.done():
            try:
                await asyncio.shield(self._join_task)
            except asyncio.CancelledError:
                # A second shutdown cancellation cannot release the original
                # journal flock/client while native writers remain physical.
                cancelled = True
        self._join_task.result()
        if cancelled:
            raise asyncio.CancelledError

    @staticmethod
    def _request(response):
        if response["stop"]:
            raise WorkspacePublicationStopped("Original publication owner was stopped")
        return response["request"]

    def _identity(self, request, claim, grant):
        values = dict(request["identity"])
        values["presented_paths"] = tuple(values["presented_paths"])
        identity = WorkspaceBoundaryIdentity(**values)
        spec = LaunchSpec.model_validate(grant["launch_spec"])
        if (
            claim["kind"] != "agent"
            or identity.attempt_id != claim["attempt_id"]
            or identity.token_stamp != hashlib.sha256(claim["token"].encode()).hexdigest()
            or identity.request_digest != request["request_digest"]
            or identity.launch_spec_digest != spec.payload_digest()
            or any(getattr(identity, name) != grant[name] for name in ("node_id", "node_session_id", "agent_task_id", "run_id", "generation", "attempt_id", "owner_worker_id", "process_ref"))
            or (identity.user_id, identity.thread_id) != (spec.user_id, spec.thread_id)
            or any(request["grant"][name] != grant[name] for name in request["grant"])
        ):
            raise ValueError("Original Node publication execution conflicts")
        return identity

    def _candidate(self, identity, grant, output_dir):
        versions = AgentWorkspaceVersions(self.nas, max_input_bytes=grant["input_limits"]["max_input_bytes"], max_output_bytes=grant["execution_profile"]["max_output_bytes"])
        recovered = versions.recover(identity)
        if recovered is not None:
            return recovered
        parts = (identity.user_id, identity.thread_id, "agents", identity.agent_task_id, identity.attempt_id)
        if Path(output_dir) != self.nas.root.joinpath(*parts):
            raise ValueError("Original Agent workspace mount conflicts")
        # Open the original user-data descriptor through the trusted NAS root;
        # no runner path or latest snapshot reference is accepted here.
        with self.nas.directory(parts) as attempt:
            source = os.dup(attempt)
            try:
                for part in (".deer-flow", "users", identity.user_id, "threads", identity.thread_id, "user-data"):
                    child = os.open(part, OPEN_DIRECTORY, dir_fd=source)
                    os.close(source)
                    source = child
                return versions.seal(identity, source)
            finally:
                os.close(source)

    @staticmethod
    def _same_container(before, after):
        names = ("container_id", "started_at", "image", "launch_fingerprint")
        runner_identity = ("pid", "ppid", "start_ticks", "uid")
        for observation in (before, after):
            runner = observation["receipt"]["runner"]
            if any(type(runner.get(name)) is not int for name in runner_identity) or not isinstance(runner.get("state"), str) or runner["state"] not in {"R", "S", "D", "T", "t", "W", "K", "P", "I"}:
                raise ValueError("Original Agent runner observation is not live and complete")
        if (
            any(before[name] != after[name] for name in names)
            or before["receipt"]["collector"]["pid_namespace"] != after["receipt"]["collector"]["pid_namespace"]
            or any(before["receipt"]["runner"][name] != after["receipt"]["runner"][name] for name in runner_identity)
            or before["receipt"]["cgroup_digest"] != after["receipt"]["cgroup_digest"]
        ):
            raise ValueError("Original Agent containment changed during copy")

    async def step(self, claim, grant, *, output_dir, record, deadline):
        if self._closing:
            raise WorkspacePublicationStopped("Original publication admission is closed")
        if time.monotonic() >= deadline:
            raise TimeoutError("Original publication deadline elapsed")
        request = self._request(await self.client.attempt(claim, "workspace/poll"))
        if request is None:
            return False
        identity = self._identity(request, claim, grant)
        pointer = record.get("workspace_candidate")
        if pointer is None or pointer["request_id"] != identity.request_id:
            pointer = {"request_id": identity.request_id, "request_digest": identity.request_digest, "barrier_epoch": request["barrier_epoch"], "nonce": request["nonce"] or token_hex(32), "manifest_id": None}
            record["workspace_candidate"] = pointer
            await self.save_record(record)
        if pointer["request_digest"] != identity.request_digest or pointer["barrier_epoch"] != request["barrier_epoch"]:
            raise ValueError("Durable original publication pointer conflicts")
        claim_fields = {name: pointer[name] for name in ("request_id", "request_digest", "barrier_epoch", "nonce")}
        request = self._request(await self.client.attempt(claim, "workspace/claim", **claim_fields))
        if request is None:
            return False
        if self._identity(request, claim, grant) != identity:
            raise ValueError("Original publication identity changed during claim")
        before = await self.containers.quiesce_workspace(grant, output_dir=output_dir, request=request, deadline=deadline)
        if self._closing:
            raise WorkspacePublicationStopped("Original publication writer admission is closed")
        key = (identity.attempt_id, identity.request_id, identity.request_digest)
        copy = self._copies.get(key)
        if copy is None:
            copy = asyncio.create_task(asyncio.to_thread(self._candidate, identity, grant, output_dir))
            self._copies[key] = copy

            def completed(actual):
                if not actual.cancelled():
                    actual.exception()
                # Only physical completion releases the owner. A later retry
                # reloads/verifies the full fixed NAS descriptor instead of
                # retaining a potentially 2MiB completed result indefinitely.
                if self._copies.get(key) is actual:
                    del self._copies[key]

            copy.add_done_callback(completed)
        async with asyncio.timeout(max(0, deadline - time.monotonic())):
            candidate = await asyncio.shield(copy)
        if pointer["manifest_id"] not in (None, candidate.manifest_id):
            raise ValueError("Durable original candidate identity conflicts")
        if request["candidate"] is not None and request["candidate"] != candidate.model_dump(mode="json"):
            raise ValueError("Prepared original descriptor conflicts")
        pointer["manifest_id"] = candidate.manifest_id
        await self.save_record(record)
        # Re-authenticate after physical I/O and renew only the identical claim.
        # The original watchdog remains the cumulative local execution bound.
        fresh = self._request(await self.client.attempt(claim, "workspace/claim", **claim_fields))
        if fresh is None:
            return False
        if self._identity(fresh, claim, grant) != identity:
            raise ValueError("Original publication identity changed during copy")
        after = await self.containers.quiesce_workspace(grant, output_dir=output_dir, request=fresh, deadline=deadline)
        self._same_container(before, after)
        if time.monotonic() >= deadline:
            raise TimeoutError("Original publication deadline elapsed")
        result = self._request(await self.client.attempt(claim, "workspace/prepared", **claim_fields, manifest=candidate.model_dump(mode="json")))
        if self._identity(result, claim, grant) != identity or result["candidate"] != candidate.model_dump(mode="json"):
            raise ValueError("Original prepared acknowledgement conflicts")
        # Only a prepared candidate. The original runner gate remains closed
        # until the separately fenced checkpoint/point transaction accepts it.
        return True
