"""Historical C07 observables adapted to original installed NodeDaemon execution."""

import asyncio
import json
import sys

from .test_c07_installed_remote_events import InstalledScenario


class DockerProcessObservation:
    """Observed namespace PID/exit; never a native host Popen claim."""

    def __init__(self):
        self.returncode = None
        self.pid = None
        self.stopped = asyncio.Event()

    async def wait(self):
        await self.stopped.wait()
        return self.returncode


class C07InstalledScenario(InstalledScenario):
    def configure(self, *, daemon, publisher, hold_stopped=False):
        self.daemon, self.publisher = daemon, publisher
        self.process = None
        self.stop_requested = asyncio.Event()
        self.stop_release = asyncio.Event()
        if not hold_stopped:
            self.stop_release.set()
        self.original_attempt = self.node.attempt

        async def held_attempt(claim, action, **kwargs):
            if action == "stopped":
                assert kwargs["physical_stopped"] is True
                physical = await self.driver.inspect(kwargs["process_ref"])
                assert not physical["State"]["Running"]
                self.inspected = physical
                self.stopped_payload = {"action": action, **kwargs, "physical_state": physical["State"], "container_id": physical["Id"]}
                self.stop_requested.set()
                await self.stop_release.wait()
            return await self.original_attempt(claim, action, **kwargs)

        self.node.attempt = held_attempt

    async def start_once(self):
        assert self.process is None and self.claim is not None
        self.process = DockerProcessObservation()
        self.execution_task = asyncio.create_task(self.daemon.execute(self.claim))

        async def observe():
            while not self.execution_task.done():
                if self.grant is not None:
                    observation = await self.driver.inspect(self.grant["process_ref"])
                    if observation and self.grant["process_ref"] in self.driver.ready and not observation["State"]["Running"]:
                        self.inspected = observation
                        self.process.returncode = observation["State"]["ExitCode"]
                        self.process.stopped.set()
                        return
                    ready = self.driver.ready.get(self.grant["process_ref"])
                    if ready:
                        self.process.pid = ready["pid"]
                await asyncio.sleep(0.05)
            await self.execution_task
            raise AssertionError("Installed C07 daemon ended without physical exit observation")

        self.physical_observer = asyncio.create_task(observe())

    async def wait_for_runner_exit(self):
        physical_wait = asyncio.create_task(self.process.wait())
        try:
            done, _ = await asyncio.wait({physical_wait, self.physical_observer}, timeout=130, return_when=asyncio.FIRST_COMPLETED)
            if self.physical_observer in done:
                await self.physical_observer  # propagate the actual observation error
            if not done:
                raise TimeoutError("Actual C07 installed physical exit observation deadline")
            await physical_wait
        finally:
            if not physical_wait.done():
                physical_wait.cancel()
            await asyncio.gather(physical_wait, return_exceptions=True)
        await asyncio.wait_for(self.stop_requested.wait(), 10)
        assert not self.inspected["State"]["Running"]
        # The stopped RPC is causally held here so tests can inspect pre-ack SQL.
        (self.directory / "physical-before-stopped-rpc.json").write_text(json.dumps(self.stopped_payload, indent=2))

    async def acknowledge_actual_stop(self):
        assert self.stop_requested.is_set() and self.process.returncode is not None
        self.stop_release.set()
        self.result = await asyncio.wait_for(asyncio.shield(self.execution_task), 30)
        return self.result

    async def start_receipts(self):
        if self.claim is None or self.grant is None:
            return []
        return await super().start_receipts()

    async def durable_receipt(self):
        receipt = await super().durable_receipt()
        receipt["execution_surface"] = "installed Linux NodeDaemon; namespace PID only"
        return receipt

    async def close(self):
        from .c08_installed_cleanup import cancel_owned_task, settle_owned_cleanup, settle_owned_execution

        original_error = sys.exc_info()[1]

        async def execution():
            if hasattr(self, "execution_task"):
                await settle_owned_execution(self.execution_task)

        async def stop():
            if self.grant is not None:
                await self.driver.stop(self.grant["process_ref"])

        async def remove():
            if self.grant is not None:
                await self.driver.command("rm", self.grant["process_ref"])

        async def diagnostics():
            if self.grant is None:
                return
            from app.fleet.runner_context import control_connection_credentials
            from deerflow.config.app_config import AppConfig

            _, stdout, stderr = await self.driver.command("logs", self.grant["process_ref"])
            raw = stdout + stderr
            for value in sorted(control_connection_credentials(AppConfig.model_validate(self.driver.operator_config)), key=len, reverse=True):
                raw = raw.replace(value, "[private control credential redacted]")
            (self.directory / "installed-runner.log").write_text(raw)

        async def observer():
            if hasattr(self, "physical_observer"):
                await cancel_owned_task(self.physical_observer)

        actions = [("release stopped RPC", self.stop_release.set)]
        if hasattr(self, "execution_task") and not self.execution_task.done() and self.grant is not None:
            actions.append(("release model barrier", self.release_model_barrier))
        actions.extend([("execution", execution), ("writers", self.publisher.join_writers), ("diagnostics", diagnostics)])
        actions.extend([("stop", stop), ("remove", remove)])
        actions.extend([("observer", observer), ("restore original stopped RPC", lambda: setattr(self.node, "attempt", self.original_attempt))])
        await settle_owned_cleanup(actions, original_error=original_error)
