"""Private infrastructure contract; importing this module does not load the harness."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from importlib import metadata
from typing import Any


@dataclass(frozen=True)
class ExecutionIdentity:
    node_id: str
    node_session_id: str
    agent_task_id: str
    generation: int
    attempt_id: str
    owner_worker_id: str
    token_stamp: str

    def __post_init__(self):
        import re

        if self.generation < 1 or not re.fullmatch(r"[a-f0-9]{64}", self.token_stamp):
            raise ValueError("Invalid private execution identity")
        if self.owner_worker_id != "fleet-agent:" + self.attempt_id:
            raise ValueError("Invalid private execution owner")
        if not all((self.node_id, self.node_session_id, self.agent_task_id, self.attempt_id)):
            raise ValueError("Incomplete private execution identity")


@dataclass(frozen=True, repr=False)
class BootstrapV1:
    identity: ExecutionIdentity
    operator_config: dict

    @classmethod
    def from_private_payload(cls, payload):
        if not isinstance(payload, dict) or set(payload) != {"schema_version", "identity", "operator_config"} or payload["schema_version"] != 1:
            raise ValueError("Unsupported private bootstrap")
        if not isinstance(payload["operator_config"], dict):
            raise ValueError("Invalid private operator configuration")
        return cls(ExecutionIdentity(**payload["identity"]), payload["operator_config"])


@dataclass(repr=False)
class AgentEnvironment:
    identity: ExecutionIdentity
    context: Any
    manager: Any
    bridge: Any
    agent_factory: Callable
    compatibility: Any
    credential_resolver: Callable
    decode_input: Callable
    close: Callable[[], Awaitable[None]]
    mutation_scope: Callable | None = None
    private_extensions_config: Any = None
    definition_stores: Any = None
    private_mcp_tools: Any = None
    private_memory_manager: Any = None
    private_mcp_task_submitter: Any = None
    workspace_scope: Callable | None = None
    execution_scope: Callable | None = None
    workspace_publications: Any = None
    observe_cancellation: Callable | None = None
    retain_executor: Callable | None = None


def installed_environment_factory(provider: str):
    """Resolve exactly one operator-selected callable from installed metadata."""
    matches = tuple(metadata.entry_points(group="deerflow.fleet.agent_environment", name=provider))
    if len(matches) != 1:
        raise RuntimeError("Agent environment provider missing or ambiguous")
    factory = matches[0].load()
    if not callable(factory):
        raise RuntimeError("Installed Agent environment factory must be callable")
    return factory


def worker_compatibility(provider: str):
    """Read nonsecret compatibility from the same selected installed factory."""
    from ..launch_spec import WorkerCompatibility

    factory = installed_environment_factory(provider)
    reader = getattr(factory, "worker_compatibility", None)
    if not callable(reader):
        raise RuntimeError("Installed Agent provider compatibility is unavailable")
    result = reader()
    if not isinstance(result, WorkerCompatibility):
        raise RuntimeError("Invalid installed Agent provider compatibility")
    return result


async def build_environment(provider: str, *, bootstrap: BootstrapV1, spec, grant):
    """Only an operator-selected installed entry point may construct infrastructure."""
    factory = installed_environment_factory(provider)
    result = await factory(bootstrap=bootstrap, spec=spec, grant=grant)
    if not isinstance(result, AgentEnvironment) or result.identity != bootstrap.identity:
        raise RuntimeError("Invalid installed Agent environment")
    return result
