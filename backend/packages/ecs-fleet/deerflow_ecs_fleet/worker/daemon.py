"""One node execution lifecycle with durable restart fencing and local stop proof."""

import asyncio
import logging
import time
from pathlib import Path

import httpx

from .journal import AttemptJournal
from .watchdog import LeaseWatchdog

logger = logging.getLogger(__name__)


class RecoveryRequired(RuntimeError):
    pass


class NodeDaemon:
    def __init__(self, *, client, containers, state_dir: Path, prepare_workspace, renew_seconds=30, safety_margin_seconds=5, poll_seconds=0.25):
        if renew_seconds <= 0 or poll_seconds <= 0 or safety_margin_seconds < 0:
            raise ValueError("Invalid worker timing")
        self.client = client
        self.containers = containers
        self.journal = AttemptJournal(state_dir)
        self.prepare_workspace = prepare_workspace
        self.renew_seconds = renew_seconds
        self.margin = safety_margin_seconds
        self.poll_seconds = poll_seconds
        self._ready = False
        self._active = set()

    async def bootstrap(self):
        self._ready = False
        await self.client.open_session()
        records = await asyncio.to_thread(self.journal.records)
        if any(row.get("node_id") != self.client.node_id for row in records):
            raise RecoveryRequired("Private attempt journal belongs to another node")
        known = {row["claim"]["attempt_id"] for row in records}
        # Stop every residual before reporting any missing journal. Failing on
        # the first orphan would leave the remaining executions running.
        managed = await self.containers.list_managed(self.client.node_id)
        refs = {ref for ref, _ in managed} | {"fleet-" + attempt_id for attempt_id in known}
        stopped = await asyncio.gather(*(self.containers.stop(ref) for ref in refs), return_exceptions=True)
        if any(result is not True for result in stopped):
            raise RecoveryRequired("Residual execution could not be stopped")
        if any(attempt_id not in known for _, attempt_id in managed):
            raise RecoveryRequired("Managed container has no private attempt journal")
        for row in records:
            ref = "fleet-" + row["claim"]["attempt_id"]
            if row.get("reported"):
                continue
            row["stop_reason"] = row.get("stop_reason", "lease_lost")
            observation = await self.containers.inspect(ref)
            row["exit_code"] = observation["State"]["ExitCode"] if observation else row.get("exit_code", 137)
            response = await self.client.attempt(row["claim"], "stopped", reason=row["stop_reason"], exit_code=row["exit_code"])
            row["reported"] = True
            row["server_state"] = response["state"]
            await asyncio.to_thread(self.journal.save, row)
        health = await self.client.heartbeat()
        if health["health"] != "online":
            raise RecoveryRequired("Node retains unresolved execution; operator reconciliation required")
        self._ready = True
        return health

    async def execute_one(self):
        if not self._ready:
            raise RecoveryRequired("Worker must reconcile before claiming execution")
        claim = await self.client.claim()
        return None if claim is None else await self.execute(claim)

    async def execute(self, claim):
        if not self._ready:
            raise RecoveryRequired("Worker must reconcile before execution")
        attempt_id = claim["attempt_id"]
        if claim["kind"] != "job" or attempt_id in self._active:
            raise ValueError("Unsupported or already active attempt")
        self._active.add(attempt_id)
        record = {"claim": claim, "node_id": self.client.node_id, "reported": False}
        launch_task = watchdog_task = renew_task = None
        ref = "fleet-" + attempt_id
        reason = "lease_lost"
        control_stop = False
        try:
            await asyncio.to_thread(self.journal.save, record)
        except BaseException:
            self._active.discard(attempt_id)
            self._ready = False
            raise
        try:
            sent = time.monotonic()
            grant = await self.client.attempt(claim, "start")
            if not grant.get("authorized") or grant["attempt_id"] != attempt_id or grant["process_ref"] != ref:
                raise ValueError("Invalid start grant")
            duration = min(grant["lease_seconds_remaining"], grant["execution_seconds_remaining"])
            remaining = duration - (time.monotonic() - sent)
            if remaining <= self.margin:
                raise ValueError("Start grant arrived after local safety deadline")
            record["grant"] = grant
            await asyncio.to_thread(self.journal.save, record)
            output = await self.prepare_workspace(claim, grant)
            if Path(self.journal.root).resolve().is_relative_to(Path(output).resolve()):
                raise ValueError("Private worker journal cannot be mounted into a job")

            async def stop_local():
                if launch_task is not None and not launch_task.done():
                    launch_task.cancel()
                    await asyncio.gather(launch_task, return_exceptions=True)
                return await self.containers.stop(ref)

            watchdog = LeaseWatchdog(stop=stop_local, lease_seconds=remaining, safety_margin_seconds=self.margin)
            watchdog.deadline = sent + duration - self.margin
            watchdog_task = asyncio.create_task(watchdog.run())
            launch_task = asyncio.create_task(self.containers.launch(grant, output_dir=output, deadline=watchdog.deadline))
            observation = await launch_task
            if observation["State"]["Status"] == "created":
                raise RecoveryRequired("A previous one-shot start intent has uncertain launch status")

            async def renew_loop():
                nonlocal reason, control_stop
                while not watchdog.expired:
                    await asyncio.sleep(self.renew_seconds)
                    sent_renew = time.monotonic()
                    try:
                        response = await self.client.attempt(claim, "renew", running=True)
                    except httpx.HTTPError as error:
                        if isinstance(error, httpx.HTTPStatusError) and error.response.status_code in {401, 403, 409}:
                            control_stop = True
                            reason = "lease_lost"
                            await stop_local()
                            return
                        continue
                    if response["stop"]:
                        control_stop = True
                        reason = "cancelled" if response["reason"] == "cancel_requested" else "execution_deadline"
                        await stop_local()
                        return
                    seconds = min(response["lease_seconds_remaining"], response["execution_seconds_remaining"])
                    if not watchdog.renew(seconds, request_started_at=sent_renew):
                        control_stop = True
                        await stop_local()
                        return

            renew_task = asyncio.create_task(renew_loop())
            while True:
                observation = await self.containers.inspect(ref)
                if observation is None or not observation["State"]["Running"]:
                    if not watchdog.expired and not control_stop:
                        reason = "exit"
                    break
                if watchdog_task.done():
                    if not await watchdog_task:
                        raise RecoveryRequired("Watchdog could not prove local stop")
                    break
                await asyncio.sleep(self.poll_seconds)
        except BaseException:
            self._ready = False
            raise
        finally:
            for task in (renew_task, watchdog_task):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(*(t for t in (renew_task, watchdog_task) if t is not None), return_exceptions=True)
            if launch_task is not None and not launch_task.done():
                launch_task.cancel()
                await asyncio.gather(launch_task, return_exceptions=True)
            self._active.discard(attempt_id)
            # Always prove physical stop before sending stopped. A failed
            # acknowledgement remains in the journal for the next bootstrap.
            if await self.containers.stop(ref):
                observation = await self.containers.inspect(ref)
                record["stop_reason"] = reason
                record["exit_code"] = observation["State"]["ExitCode"] if observation else 137
                await asyncio.to_thread(self.journal.save, record)
                try:
                    response = await self.client.attempt(claim, "stopped", reason=reason, exit_code=record["exit_code"])
                except httpx.HTTPError:
                    record["reported"] = False
                else:
                    record["reported"] = True
                    record["server_state"] = response["state"]
                    await asyncio.to_thread(self.journal.save, record)
        if not record["reported"] or record.get("server_state") == "unknown":
            self._ready = False
        return {"attempt_id": attempt_id, "report_pending": not record["reported"], "state": record.get("server_state"), "stop_reason": reason}

    async def run(self, *, stop: asyncio.Event, max_parallel=1):
        if max_parallel < 1:
            raise ValueError("Invalid local worker parallelism")
        await self.bootstrap()
        active = set()
        try:
            while not stop.is_set():
                for task in tuple(active):
                    if task.done():
                        active.remove(task)
                        try:
                            task.result()
                        except Exception:
                            self._ready = False
                            logger.error("Fleet execution requires reconciliation")
                if not self._ready:
                    raise RecoveryRequired("Worker requires reconciliation before new claims")
                health = await self.client.heartbeat()
                if health["health"] != "online":
                    self._ready = False
                    raise RecoveryRequired("Node retains unresolved execution")
                if len(active) < max_parallel:
                    claim = await self.client.claim()
                    if claim is not None:
                        active.add(asyncio.create_task(self.execute(claim)))
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.poll_seconds)
                except TimeoutError:
                    pass
        finally:
            self._ready = False
            for task in active:
                task.cancel()
            await asyncio.gather(*active, return_exceptions=True)
