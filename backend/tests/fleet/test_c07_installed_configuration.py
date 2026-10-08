"""Source-level installed capsule prerequisites, independent of native relocation."""

import ast
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def installed_capsule_configuration():
    source = Path(__file__).with_name("test_c07_installed_remote_events.py")
    tree = ast.parse(source.read_text())
    function = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name.startswith("test_installed_original_agent"))
    private = next(node.value for node in ast.walk(function) if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "private" for target in node.targets))
    db = SimpleNamespace(runner_url="postgresql://fixture:sql-only-7b541fc2@host.docker.internal:15436/fixture", schema="fixture")
    config = eval(compile(ast.Expression(private), str(source), "eval"), {"db": db})
    backend = next(node for node in ast.walk(function) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "FleetExecutionBackend")
    selected = {keyword.arg: ast.literal_eval(keyword.value) for keyword in backend.keywords if keyword.arg in {"model_name", "model_version", "secret_refs"}}
    return config, selected


def test_installed_capsule_matches_actual_prepared_model_bindings_and_private_targets(tmp_path, monkeypatch):
    import app.fleet.runner_context as host
    from app.fleet.runner_context import validate_model_bindings
    from deerflow.config.app_config import AppConfig

    context = tmp_path / "prepared"
    subprocess.run([sys.executable, str(Path(__file__).with_name("build_c07_runner_image.py")), "--context", str(context), "--tag", "unused-source-prerequisite", "--prepare-only"], check=True)
    private, selected = installed_capsule_configuration()
    config = AppConfig.model_validate(private)
    bindings = json.loads((context / "model-bindings.json").read_text())
    validate_model_bindings(config, SimpleNamespace(**selected), bindings)
    assert set(bindings) == {"model-1", "child"}
    assert selected["model_name"] == "model-1"
    assert selected["model_version"] == "v1"
    bundle = json.loads((context / "runtime-bundle.json").read_text())
    assert {ref["reference_id"] for ref in selected["secret_refs"]} == {"operator-model-binding", "operator-child-binding", "operator-mcp-binding"}
    assert set(bundle["secret_bindings"]) == {"operator-model-binding", "operator-child-binding", "operator-mcp-binding"}
    assert config.extensions.get_enabled_mcp_servers()["c04"].command == "/usr/local/bin/python"
    assert config.extensions.get_enabled_mcp_servers()["c04"].args == ["-m", "fleet.c04_mcp_fixture"]
    assert config.extensions.get_enabled_mcp_servers()["c04"].env["ERP_AUTH"]
    assert config.plugins[0].use == "deerflow_c04_fixture:install"
    monkeypatch.setattr(host, "runtime_bundle", lambda: ("prepared-fixture", bundle))
    host.validate_runtime_configuration(config)
    secret_refs = [SimpleNamespace(**ref) for ref in selected["secret_refs"]]
    assert host.validate_secret_bindings(config, SimpleNamespace(secret_refs=secret_refs), bundle) == ({"model-1", "child"}, {"c04"})


@pytest.mark.parametrize("c07_gate", [False, True])
def test_scripted_provider_gate_preserves_credential_requirement(c07_gate):
    from fleet.c04_worker_fixture import ScriptedModel

    with pytest.raises(ValueError, match="Approved provider credential required"):
        ScriptedModel(c07_gate=c07_gate).reply([])


@pytest.mark.parametrize("c07_gate", [False, True])
def test_scripted_provider_gate_preserves_trusted_infrastructure_scope_requirement(c07_gate):
    from fleet.c04_worker_fixture import ScriptedModel

    with pytest.raises(ValueError, match="Trusted infrastructure scopes were not propagated"):
        ScriptedModel(api_key="fixture-provider-binding", c07_gate=c07_gate).reply([])
