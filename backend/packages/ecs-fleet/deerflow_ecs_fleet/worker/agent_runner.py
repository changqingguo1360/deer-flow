"""Agent-only harness adapter, loaded after the standalone Linux hardening entry."""

import argparse
import asyncio
import json
import os
import socket
from types import SimpleNamespace

from ..launch_spec import LaunchSpec
from .agent_environment import BootstrapV1, build_environment


class AgentRunner:
    async def run(self, spec: LaunchSpec, *, grant, environment):
        from contextlib import ExitStack

        with ExitStack() as scopes:
            for name in ("mutation_scope", "workspace_scope", "execution_scope"):
                scope = getattr(environment, name, None)
                if scope is not None:
                    scopes.enter_context(scope())
            return await self._run(spec, grant=grant, environment=environment)

    async def _run(self, spec: LaunchSpec, *, grant, environment):
        executor = asyncio.create_task(self._execute(spec, grant=grant, environment=environment), name="original-agent-executor-" + spec.run_id)
        observer = None
        retain = getattr(environment, "retain_executor", None)
        if retain is not None:
            retain(executor)
        observe = getattr(environment, "observe_cancellation", None)
        if observe is not None:

            async def observe_original():
                try:
                    await observe()
                except asyncio.CancelledError:
                    raise
                except BaseException as error:
                    import logging
                    import traceback

                    frames = [
                        (frame.name, frame.lineno) for frame in traceback.extract_tb(error.__traceback__) if frame.name in {"observe_original", "observe", "observe_original_control", "validate", "execute", "__aexit__", "__aenter__", "read"}
                    ]
                    logging.getLogger(__name__).warning("Original Agent control observer failed: error_type=%s frames=%s", type(error).__name__, frames)
                    await environment.manager.mark_execution_ownership_lost(spec.run_id)
                    executor.cancel()
                    raise

            observer = asyncio.create_task(observe_original(), name="original-agent-control-" + spec.run_id)
            if retain is not None:
                retain(observer, role="control-observer")
        try:
            while True:
                try:
                    return await asyncio.shield(executor)
                except asyncio.CancelledError:
                    if executor.done():
                        return executor.result()
                    await environment.manager.signal_execution_cancel(spec.run_id, action="interrupt")
        finally:
            if executor.done() and retain is not None:
                retain(None)
            if observer is not None:
                observer.cancel()
                joined = asyncio.gather(observer, return_exceptions=True)
                while not joined.done():
                    try:
                        await asyncio.shield(joined)
                    except asyncio.CancelledError:
                        continue
                if retain is not None:
                    retain(None, role="control-observer")

    async def _execute(self, spec: LaunchSpec, *, grant, environment):
        from contextlib import ExitStack

        from deerflow.config.extensions_config import extensions_config_scope
        from deerflow.mcp.cache import mcp_tools_scope
        from deerflow.models.credentials import model_credential_scope
        from deerflow.persistence.agent_definition_context import agent_definition_store_scope
        from deerflow.runtime.runs.worker import run_agent
        from deerflow.runtime.user_context import reset_current_user, set_current_user

        identity = environment.identity
        expected = {
            "node_id": identity.node_id,
            "node_session_id": identity.node_session_id,
            "attempt_id": identity.attempt_id,
            "owner_worker_id": identity.owner_worker_id,
            "agent_task_id": identity.agent_task_id,
            "generation": identity.generation,
            "run_id": spec.run_id,
        }
        if not grant.get("authorized") or any(grant.get(key) != value for key, value in expected.items()):
            raise ValueError("Agent authorization identity mismatch")
        if spec.agent_task_id != identity.agent_task_id or spec.generation != identity.generation:
            raise ValueError("Agent launch identity mismatch")
        if (spec.runtime_digest, spec.skill_snapshot, spec.plugin_snapshot) != (environment.compatibility.runtime_digest, environment.compatibility.skill_snapshot, environment.compatibility.plugin_snapshot):
            raise ValueError("Installed Agent runtime is incompatible")
        record = await environment.manager.attach_existing_executor(
            spec.run_id,
            user_id=spec.user_id,
            thread_id=spec.thread_id,
            owner_worker_id=identity.owner_worker_id,
            execution_backend="fleet",
            task=asyncio.current_task(),
        )
        # Input decoding belongs to the trusted host provider. The optional Fleet
        # package never imports app; it only receives the resulting runtime object.
        graph_input = environment.decode_input(spec.input)
        token = set_current_user(SimpleNamespace(id=spec.user_id))
        try:
            # Store-only admission does not enter the Gateway's local metadata
            # worker. Initialize this original thread through the bound writer
            # before its first status update, never adopting another owner.
            thread_store = environment.context.thread_store
            if thread_store is not None:
                await thread_store.ensure_executor_thread(spec.thread_id, assistant_id=record.assistant_id, metadata=record.metadata, user_id=spec.user_id)
            with ExitStack() as scopes:
                scopes.enter_context(model_credential_scope(environment.credential_resolver))
                if environment.private_extensions_config is not None:
                    scopes.enter_context(extensions_config_scope(environment.private_extensions_config))
                if environment.definition_stores is not None:
                    scopes.enter_context(agent_definition_store_scope(*environment.definition_stores))
                if environment.private_mcp_task_submitter is not None:
                    from deerflow.mcp.tasks.runtime import mcp_task_submitter_scope

                    scopes.enter_context(mcp_task_submitter_scope(environment.private_mcp_task_submitter, environment.private_extensions_config))
                if environment.private_memory_manager is not None:
                    from deerflow.agents.memory.manager import memory_manager_scope

                    scopes.enter_context(memory_manager_scope(environment.private_memory_manager))
                if environment.private_mcp_tools is not None:
                    scopes.enter_context(mcp_tools_scope(environment.private_mcp_tools))
                await run_agent(
                    bridge=environment.bridge,
                    run_manager=environment.manager,
                    record=record,
                    ctx=environment.context,
                    agent_factory=environment.agent_factory,
                    graph_input=graph_input,
                    config=spec.canonical_payload()["normalized_config"],
                    stream_modes=list(spec.stream_modes),
                    stream_subgraphs=spec.stream_subgraphs,
                    interrupt_before=spec.interrupt_before if isinstance(spec.interrupt_before, str) or spec.interrupt_before is None else list(spec.interrupt_before),
                    interrupt_after=spec.interrupt_after if isinstance(spec.interrupt_after, str) or spec.interrupt_after is None else list(spec.interrupt_after),
                )
        finally:
            reset_current_user(token)
        return record


async def _settle_pending_cleanup(pending, original_error=None):
    from .agent_cleanup import PendingAgentCleanup, abort_isolated_cleanup

    original = pending
    while isinstance(pending, PendingAgentCleanup):
        if pending.remaining_seconds <= 0:
            # Ordinary embedded callers retain and report pending. Only the
            # dedicated entry installs a physical self-exit callback.
            abort_isolated_cleanup()
            raise pending
        with pending.cleanup_scope():
            try:
                await pending.wait_for_cleanup()
                await pending.retry_cleanup()
            except PendingAgentCleanup as next_pending:
                pending = next_pending
                continue
            except BaseException as cleanup_error:
                if original_error is not None:
                    raise original_error from cleanup_error
                raise
        if original_error is not None:
            raise original_error
        raise original


async def _bootstrap(payload, provider):
    if not isinstance(payload, dict) or set(payload) != {"bootstrap", "grant"}:
        raise ValueError("Invalid private Agent invocation")
    bootstrap = BootstrapV1.from_private_payload(payload["bootstrap"])
    grant = payload["grant"]
    spec = LaunchSpec.model_validate(grant["launch_spec"])
    from .agent_cleanup import PendingAgentCleanup

    try:
        environment = await build_environment(provider, bootstrap=bootstrap, spec=spec, grant=grant)
    except PendingAgentCleanup as pending:
        await _settle_pending_cleanup(pending, getattr(pending, "_original_error", None))
        raise
    original_error = None
    try:
        print(json.dumps({"ready": True, "pid": os.getpid(), "host": socket.gethostname(), "attempt_id": bootstrap.identity.attempt_id, "uid": os.getuid(), "pid_namespace": os.readlink("/proc/self/ns/pid")}), flush=True)
        await AgentRunner().run(spec, grant=grant, environment=environment)
    except BaseException as error:
        original_error = error
        raise
    finally:
        try:
            await environment.close()
        except PendingAgentCleanup as pending:
            await _settle_pending_cleanup(pending, original_error)
        except BaseException as cleanup_error:
            if original_error is not None:
                raise original_error from cleanup_error
            raise


def bootstrap_main(payload, *, argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", required=True)
    args = parser.parse_args(argv)
    asyncio.run(_bootstrap(payload, args.provider))
    return 0


class _IsolatedCleanupWatchdog:
    """Physical bound only for the dedicated Agent process entry."""

    def __init__(self):
        import threading

        self._condition = threading.Condition()
        self._deadline = None
        self._finished = False

    def observe(self, deadline):
        import threading

        with self._condition:
            if self._deadline is None:
                self._deadline = deadline
                threading.Thread(target=self._run, name="agent-cleanup-deadline", daemon=True).start()
            else:
                # An additional phase or provider can never extend the first
                # physical deadline observed in this private process.
                self._deadline = min(self._deadline, deadline)
                self._condition.notify_all()

    def _run(self):
        import time

        with self._condition:
            while not self._finished:
                remaining = self._deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            if self._finished:
                return
        self.abort()

    @staticmethod
    def abort():
        from .agent_cleanup import CLEANUP_DEADLINE_EXIT_CODE

        # No successful finalization, stack unwind, or implicit asyncio.run
        # cancel-all can occur after the dedicated worker exceeds its budget.
        os._exit(CLEANUP_DEADLINE_EXIT_CODE)

    def finish(self):
        with self._condition:
            self._finished = True
            self._condition.notify_all()


def isolated_bootstrap_main(payload, *, argv):
    """Called only by the hardened standalone Agent entry, never the host."""
    from .agent_cleanup import isolated_cleanup_policy

    watchdog = _IsolatedCleanupWatchdog()
    with isolated_cleanup_policy(watchdog.observe, watchdog.abort):
        try:
            return bootstrap_main(payload, argv=argv)
        finally:
            watchdog.finish()
