"""Exact contracts from the read-only installed runtime, never client config."""

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

WORKSPACE_CONTRACT_PATH = "/opt/deerflow/workspace-contracts.json"
HOST_USE = "app.fleet.runner_context:build_agent_environment"
SANDBOX_USE = "deerflow.sandbox.local:LocalSandboxProvider"
HOST_CONTRACT = "host-supervised"
PLUGIN_CONTRACT = "fenced-no-retained-user-data-writer"
MCP_CONTRACT = "stateless-reconnectable-no-retained-user-data-writer"
FORBIDDEN_MCP_FLAGS = frozenset({"--token", "--password", "--api-key", "--api_key", "--authorization", "--dsn", "--database-url", "--redis-url"})


def validate_mcp_binding(binding):
    """Check approved non-secret connection shape before awarding capability."""
    if not isinstance(binding, dict):
        raise ValueError("Invalid installed MCP connection binding")
    transport = binding.get("transport")
    if transport == "stdio":
        if set(binding) != {"transport", "command", "args", "allowed_env_keys"}:
            raise ValueError("Installed MCP binding cannot contain resolved credentials")
        command, args, keys = binding["command"], binding["args"], binding["allowed_env_keys"]
        if (
            not isinstance(command, str)
            or not Path(command).is_absolute()
            or ".." in Path(command).parts
            or not isinstance(args, list)
            or any(not isinstance(arg, str) or arg.split("=", 1)[0].lower() in FORBIDDEN_MCP_FLAGS for arg in args)
            or not isinstance(keys, list)
            or any(not isinstance(key, str) or not key for key in keys)
            or len(set(keys)) != len(keys)
        ):
            raise ValueError("Invalid installed MCP process binding")
    elif transport in {"http", "sse"}:
        if set(binding) != {"transport", "url"} or not isinstance(binding["url"], str):
            raise ValueError("Installed MCP endpoint binding cannot contain resolved credentials")
        parsed = urlsplit(binding["url"])
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or any(part in key.lower() for key in parse_qs(parsed.query) for part in ("token", "key", "secret", "password", "credential"))
        ):
            raise ValueError("Installed MCP endpoint requires private scoped authentication")
    else:
        raise ValueError("Unsupported installed MCP transport")


@dataclass(frozen=True)
class InstalledWorkspaceContracts:
    version: int
    mcp_servers: tuple[str, ...]


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate installed workspace contract field")
        result[key] = value
    return result


def validate_workspace_contracts(raw, *, bundle, sandbox_use):
    if not isinstance(raw, bytes) or len(raw) > 2 * 1024 * 1024:
        raise ValueError("Installed workspace contracts exceed bound")
    try:
        value = json.loads(raw, object_pairs_hook=_object)
    except (ValueError, UnicodeError):
        raise ValueError("Invalid installed workspace contracts") from None
    if not isinstance(value, dict) or set(value) != {"schema_version", "host", "plugins", "mcp_servers"} or type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("Unsupported installed workspace contracts")
    if sandbox_use != SANDBOX_USE or value["host"] != {"use": HOST_USE, "sandbox_use": sandbox_use, "contract": HOST_CONTRACT}:
        raise ValueError("Unsupported original workspace host/sandbox contract")
    plugins = value["plugins"]
    expected = {item["name"]: {**item, "contract": PLUGIN_CONTRACT} for item in bundle["plugins"]}
    if (
        not isinstance(plugins, list)
        or len(plugins) != len(expected)
        or any(not isinstance(item, dict) or item.get("name") not in expected or item != expected[item["name"]] for item in plugins)
        or len({item["name"] for item in plugins}) != len(expected)
    ):
        raise ValueError("Installed workspace plugin contracts differ from original bundle")
    servers = value["mcp_servers"]
    if not isinstance(servers, dict) or set(servers) != set(bundle["mcp_servers"]):
        raise ValueError("Installed workspace MCP contracts differ from original bundle")
    for name, binding in bundle["mcp_servers"].items():
        validate_mcp_binding(binding)
        references = {reference: item for reference, item in bundle["secret_bindings"].items() if item.get("kind") == "mcp" and item.get("target") == name}
        if any(set(item) != {"name", "kind", "target"} or any(not isinstance(part, str) or not part for part in (reference, *item.values())) for reference, item in references.items()):
            raise ValueError("Installed workspace bindings may contain only original secret references")
        if servers[name] != {"binding": binding, "secret_bindings": references, "contract": MCP_CONTRACT}:
            raise ValueError("Installed workspace MCP connection/reference contract conflicts")
    return InstalledWorkspaceContracts(1, tuple(sorted(servers)))


def read_workspace_contracts(*, bundle, sandbox_use):
    """Read only the fixed installed file without following a leaf symlink."""
    fd = os.open(WORKSPACE_CONTRACT_PATH, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 2 * 1024 * 1024:
            raise ValueError("Unsafe installed workspace contracts")
        raw = stream.read(2 * 1024 * 1024 + 1)
    return raw, validate_workspace_contracts(raw, bundle=bundle, sandbox_use=sandbox_use)
