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
        from contextlib import nullcontext

        scope = getattr(environment, "mutation_scope", None)
        with scope() if scope is not None else nullcontext():
            return await self._run(spec, grant=grant, environment=environment)

    async def _run(self, spec: LaunchSpec, *, grant, environment):
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


async def _bootstrap(payload, provider):
    if not isinstance(payload, dict) or set(payload) != {"bootstrap", "grant"}:
        raise ValueError("Invalid private Agent invocation")
    bootstrap = BootstrapV1.from_private_payload(payload["bootstrap"])
    grant = payload["grant"]
    spec = LaunchSpec.model_validate(grant["launch_spec"])
    environment = await build_environment(provider, bootstrap=bootstrap, spec=spec, grant=grant)
    try:
        print(json.dumps({"ready": True, "pid": os.getpid(), "host": socket.gethostname(), "attempt_id": bootstrap.identity.attempt_id, "uid": os.getuid(), "pid_namespace": os.readlink("/proc/self/ns/pid")}), flush=True)
        await AgentRunner().run(spec, grant=grant, environment=environment)
    finally:
        await environment.close()


def bootstrap_main(payload, *, argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", required=True)
    args = parser.parse_args(argv)
    asyncio.run(_bootstrap(payload, args.provider))
    return 0
