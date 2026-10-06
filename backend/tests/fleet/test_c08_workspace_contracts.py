"""Installed contracts are exact host bindings, never extension-config traits."""

import copy
import importlib
import json

import pytest


def actual_bundle():
    return {
        "skills": [],
        "plugins": [{"name": "original-plugin", "use": "original_plugin:install", "distribution": "original-plugin"}],
        "mcp_servers": {"original-mcp": {"transport": "stdio", "command": "/opt/runtime/bin/python", "args": ["/opt/fixture/server.py"], "allowed_env_keys": ["TARGET_AUTH"]}},
        "secret_bindings": {"original-target-reference": {"name": "TARGET_AUTH", "kind": "mcp", "target": "original-mcp"}},
    }


def installed_contract():
    bundle = actual_bundle()
    return {
        "schema_version": 1,
        "host": {"use": "app.fleet.runner_context:build_agent_environment", "sandbox_use": "deerflow.sandbox.local:LocalSandboxProvider", "contract": "host-supervised"},
        "plugins": [{**bundle["plugins"][0], "contract": "fenced-no-retained-user-data-writer"}],
        "mcp_servers": {"original-mcp": {"binding": bundle["mcp_servers"]["original-mcp"], "secret_bindings": bundle["secret_bindings"], "contract": "stateless-reconnectable-no-retained-user-data-writer"}},
    }


def validator():
    try:
        module = importlib.import_module("app.fleet.workspace_contracts")
    except ModuleNotFoundError:
        module = None
    assert module is not None, "trusted installed workspace contracts are not enforced"
    return module.validate_workspace_contracts


def test_installed_contract_matches_exact_original_runtime_bindings():
    result = validator()(json.dumps(installed_contract()).encode(), bundle=actual_bundle(), sandbox_use="deerflow.sandbox.local:LocalSandboxProvider")
    assert result.version == 1
    assert result.mcp_servers == ("original-mcp",)


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown-field",
        "host-use",
        "sandbox-use",
        "host-trait",
        "missing-plugin",
        "extra-plugin",
        "plugin-use",
        "plugin-distribution",
        "plugin-trait",
        "missing-mcp",
        "extra-mcp",
        "mcp-command",
        "mcp-args",
        "mcp-env",
        "mcp-trait",
        "client-trait",
        "duplicate-plugin",
        "version-bool",
    ],
)
def test_installed_contract_rejects_missing_extra_or_drifting_bindings(mutation):
    contract = copy.deepcopy(installed_contract())
    if mutation == "unknown-field":
        contract["client_claim"] = True
    elif mutation in {"host-use", "sandbox-use", "host-trait"}:
        key = {"host-use": "use", "sandbox-use": "sandbox_use", "host-trait": "contract"}[mutation]
        contract["host"][key] = "client-claimed-safe"
    elif mutation == "missing-plugin":
        contract["plugins"] = []
    elif mutation == "extra-plugin":
        contract["plugins"].append({"name": "other", "use": "other:install", "distribution": "other", "contract": "fenced-no-retained-user-data-writer"})
    elif mutation == "duplicate-plugin":
        contract["plugins"].append(contract["plugins"][0])
    elif mutation.startswith("plugin-"):
        contract["plugins"][0][mutation.split("-", 1)[1]] = "changed"
    elif mutation == "missing-mcp":
        contract["mcp_servers"] = {}
    elif mutation == "extra-mcp":
        contract["mcp_servers"]["extra"] = contract["mcp_servers"]["original-mcp"]
    elif mutation in {"mcp-command", "mcp-args", "mcp-env"}:
        key = {"mcp-command": "command", "mcp-args": "args", "mcp-env": "allowed_env_keys"}[mutation]
        contract["mcp_servers"]["original-mcp"]["binding"][key] = "changed"
    elif mutation == "mcp-trait":
        contract["mcp_servers"]["original-mcp"]["contract"] = "client-claimed-safe"
    elif mutation == "client-trait":
        contract["mcp_servers"]["original-mcp"]["workspace_safe"] = True
    else:
        contract["schema_version"] = True
    with pytest.raises(ValueError):
        validator()(json.dumps(contract).encode(), bundle=actual_bundle(), sandbox_use="deerflow.sandbox.local:LocalSandboxProvider")


def test_legacy_worker_abi_stays_parseable_but_has_no_workspace_capability():
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility

    old = WorkerCompatibility(runtime_digest="sha256:" + "a" * 64, skill_snapshot={"entries": []}, plugin_snapshot={"entries": []})
    assert getattr(old, "workspace_contract_version", None) is None


@pytest.mark.asyncio
async def test_original_container_preflight_rejects_legacy_worker_abi(tmp_path, monkeypatch):
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers

    containers = AgentContainers(state_dir=tmp_path / "private", operator_config={})

    async def original_inspection(*args, **kwargs):
        return json.dumps({"runtime_digest": "sha256:" + "a" * 64, "skill_snapshot": {"entries": []}, "plugin_snapshot": {"entries": []}})

    monkeypatch.setattr(containers, "checked", original_inspection)
    with pytest.raises(ValueError, match="workspace"):
        await containers.compatibility("original-image")


from .test_c02_remote_agent_admission import admission as admission  # noqa: E402,F401
from .test_c03_remote_agent_admission import owner_environment as owner_environment  # noqa: E402,F401


@pytest.mark.asyncio
async def test_actual_agent_claim_rejects_legacy_workspace_abi_before_ownership_write(owner_environment):  # noqa: F811
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
    from sqlalchemy import text

    engine, _, _, _, app, _, session_id, _, original = owner_environment
    legacy = WorkerCompatibility(runtime_digest=original.runtime_digest, skill_snapshot=original.skill_snapshot, plugin_snapshot=original.plugin_snapshot)
    async with engine.connect() as connection:
        before = (await connection.execute(text("SELECT row_to_json(t)::text FROM runs t"))).all()
    with pytest.raises(ValueError, match="workspace"):
        await app.state.fleet_ownership.claim_agent("node-c03", node_session_id=session_id, worker=legacy)
    async with engine.connect() as connection:
        after = (await connection.execute(text("SELECT row_to_json(t)::text FROM runs t"))).all()
        assert (await connection.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
    assert after == before


@pytest.mark.asyncio
async def test_original_daemon_physically_stops_journal_before_rejecting_legacy_new_claim(tmp_path):
    import subprocess
    import sys

    import httpx
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_ecs_fleet.worker.journal import AttemptJournal

    events = []
    child = subprocess.Popen([sys.executable, "-I", "-S", "-c", "import time; time.sleep(30)"])

    class OwnedContainers:
        async def list_managed(self, node_id):
            assert node_id == "original-node"
            return [("fleet-original-attempt", "original-attempt")]

        async def stop(self, ref):
            assert ref == "fleet-original-attempt"
            child.terminate()
            child.wait(timeout=2)
            events.append("physical-original-stop")
            return child.poll() is not None

        async def inspect(self, ref):
            return {"State": {"ExitCode": 137}}

    async def transport(request):
        path = request.url.path
        if path.endswith("/session"):
            return httpx.Response(200, json={"node_id": "original-node", "node_session_id": "original-session"})
        if path.endswith("/reconcile-stopped"):
            body = __import__("json").loads(request.content)
            assert body["node_session_id"] == "original-session"
            assert body["original_node_session_id"] == "old-original-session"
            assert body["process_ref"] == "fleet-original-attempt"
            assert body["physical_stopped"] is True
            assert child.poll() is not None
            events.append("original-stop-report")
            return httpx.Response(200, json={"state": "stopped"})
        if path.endswith("/heartbeat"):
            return httpx.Response(200, json={"health": "online"})
        pytest.fail("Legacy compatibility must reject before a new claim HTTP request")

    async def installed_preflight():
        events.append("installed-new-claim-preflight")
        raise ValueError("workspace contract capability absent")

    journal = AttemptJournal(tmp_path / "private")
    journal.save({"claim": {"kind": "agent", "attempt_id": "original-attempt", "token": "original-private-fixture-token"}, "node_id": "original-node", "grant": {"node_session_id": "old-original-session"}, "reported": False})
    async with httpx.AsyncClient(base_url="http://test/", transport=httpx.MockTransport(transport)) as http:
        try:
            client = NodeClient(gateway_url="http://test/", credential="df_fleet_private-fixture", http_client=http, claim_kind="agent", compatibility_loader=installed_preflight)
            daemon = NodeDaemon(client=client, containers=OwnedContainers(), state_dir=tmp_path / "private")
            await daemon.bootstrap()
            with pytest.raises(ValueError, match="workspace"):
                await daemon.execute_one()
            assert events == ["physical-original-stop", "original-stop-report", "installed-new-claim-preflight"]
            assert journal.records()[0]["reported"] is True
            assert child.poll() is not None
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=2)


@pytest.mark.parametrize("value", [True, False, "1", 2])
def test_workspace_capability_cannot_be_coerced_from_untrusted_marker(value):
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility

    with pytest.raises(ValueError):
        WorkerCompatibility(runtime_digest="sha256:" + "a" * 64, skill_snapshot={"entries": []}, plugin_snapshot={"entries": []}, workspace_contract_version=value)


from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner  # noqa: E402,F401


@pytest.mark.asyncio
async def test_original_host_missing_installed_contract_rejects_before_resource_or_graph_effects(checkpoint_owner, tmp_path, monkeypatch):  # noqa: F811
    from pathlib import Path
    from types import SimpleNamespace

    import app.fleet.runner_context as host
    import app.fleet.workspace_contracts as contracts
    from deerflow.config.app_config import AppConfig

    item = checkpoint_owner
    private = AppConfig.model_validate({**item.private.model_dump(), "agent_storage": {"backend": "db"}, "run_events": {"backend": "db"}, "memory": {"enabled": False, "manager_class": "noop"}})
    monkeypatch.setattr(host, "validate_model_bindings", lambda *args: None)
    monkeypatch.setattr(host, "validate_runtime_configuration", lambda *args: None)
    monkeypatch.setattr(host, "runtime_bundle", lambda: (b"{}", {"skills": [], "plugins": [], "mcp_servers": {}, "secret_bindings": {}}))
    original_read = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda path: b"{}" if str(path) == "/opt/deerflow/model-bindings.json" else original_read(path))
    monkeypatch.setattr(contracts, "WORKSPACE_CONTRACT_PATH", str(tmp_path / "missing-installed-contract.json"))

    def forbidden(*args, **kwargs):
        pytest.fail("Missing installed contract must reject before host resources or graph construction")

    monkeypatch.setattr(host, "execution_configuration", forbidden)
    with pytest.raises(FileNotFoundError):
        await host.build_agent_environment(bootstrap=SimpleNamespace(identity=item.identity, operator_config=private.model_dump()), spec=item.spec, grant=item.grant)


@pytest.mark.parametrize(
    "binding",
    [
        {"transport": "stdio", "command": "/opt/bin/python", "args": [], "allowed_env_keys": [], "env": {"TARGET_AUTH": "must-not-store-resolved-credential"}},
        {"transport": "stdio", "command": "python", "args": [], "allowed_env_keys": []},
        {"transport": "stdio", "command": "/opt/bin/python", "args": ["--password=must-not-store"], "allowed_env_keys": []},
        {"transport": "http", "url": "https://user:must-not-store@example.com/mcp"},
        {"transport": "http", "url": "https://example.com/mcp?api_key=must-not-store"},
        {"transport": "http", "url": "https://example.com/mcp", "headers": {"Authorization": "must-not-store"}},
    ],
)
def test_matching_installed_contract_does_not_approve_invalid_or_secret_bundle_binding(binding):
    bundle = actual_bundle()
    bundle["mcp_servers"]["original-mcp"] = binding
    contract = installed_contract()
    contract["mcp_servers"]["original-mcp"]["binding"] = binding
    with pytest.raises(ValueError):
        validator()(json.dumps(contract).encode(), bundle=bundle, sandbox_use="deerflow.sandbox.local:LocalSandboxProvider")


def test_actual_installed_capability_rejects_unsupported_kernel_before_marker(tmp_path, monkeypatch):
    from pathlib import Path
    from types import SimpleNamespace

    from deerflow_ecs_fleet.worker import workspace_collector as collector

    import app.fleet.runner_context as host

    from .c08_contract_fixture import install_empty_workspace_contract

    private = SimpleNamespace(extensions=SimpleNamespace(get_enabled_mcp_servers=lambda: {}), plugins=[], sandbox=SimpleNamespace(use="deerflow.sandbox.local:LocalSandboxProvider"))
    bundle = install_empty_workspace_contract(monkeypatch, tmp_path, private)
    monkeypatch.setattr(host, "runtime_bundle", lambda: bundle)
    monkeypatch.setattr(host, "_distribution_files", lambda name: (None, {}))
    bindings = tmp_path / "model-bindings.json"
    bindings.write_text("{}")
    skills = tmp_path / "skills"
    skills.mkdir()
    paths = {"/opt/deerflow/model-bindings.json": bindings, "/opt/deerflow/skills": skills}
    monkeypatch.setattr(host, "Path", lambda value: paths.get(str(value), Path(value)))

    def unsupported():
        raise ValueError("Actual Linux kernel capability unavailable")

    monkeypatch.setattr(collector, "kernel_capability", unsupported)
    with pytest.raises(ValueError, match="kernel capability"):
        host.installed_compatibility()
