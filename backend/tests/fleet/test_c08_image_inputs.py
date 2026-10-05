"""Source-only preparation; no image build or Linux claim in this suite."""

import hashlib
import importlib
import json
from pathlib import Path


def test_new_c08_prepared_inputs_bind_fixed_collector_and_exact_original_bundle(tmp_path, monkeypatch):
    from deerflow_ecs_fleet.worker import workspace_collector

    from app.fleet.workspace_contracts import SANDBOX_USE, validate_workspace_contracts

    builder = importlib.import_module(".build_c08_runner_image", __package__)
    monkeypatch.setattr(builder.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Source preparation must not run Docker/build")))
    context = tmp_path / "fresh-inputs"
    builder.prepare_context(context)
    bundle = json.loads((context / "runtime-bundle.json").read_bytes())
    assert set(bundle) == {"skills", "plugins", "mcp_servers", "secret_bindings"}
    contracts = (context / "workspace-contracts.json").read_bytes()
    assert validate_workspace_contracts(contracts, bundle=bundle, sandbox_use=SANDBOX_USE).version == 1
    assert {plugin["name"] for plugin in bundle["plugins"]} == {"c04-plugin"}
    assert set(bundle["mcp_servers"]) == {"c04"}
    collector = (context / "libexec_workspace_collector.py").read_bytes()
    assert collector == Path(workspace_collector.__file__).read_bytes()
    inventory = json.loads((context / "prepared-inputs.json").read_bytes())
    assert inventory["libexec_workspace_collector.py"] == hashlib.sha256(collector).hexdigest()
    assert inventory["workspace-contracts.json"] == hashlib.sha256(contracts).hexdigest()
    dockerfile = (context / "Dockerfile").read_text()
    assert "libexec_workspace_collector.py workspace-contracts.json" in dockerfile
    assert "deerflow-c08-dependencies:local" in dockerfile


def test_stock_boundary_observer_is_part_of_fresh_installed_inputs(tmp_path, monkeypatch):
    builder = importlib.import_module(".build_c08_runner_image", __package__)
    monkeypatch.setattr(builder.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Source preparation must not build")))
    context = builder.prepare_context(tmp_path / "stock-inputs")
    inventory = json.loads((context / "prepared-inputs.json").read_bytes())
    assert "c08_stock_linux_fixture.py" in inventory
    source = Path(__file__).parent / "c08_stock_linux_fixture.py"
    assert (context / source.name).read_bytes() == source.read_bytes()
    assert source.name in (context / "Dockerfile").read_text()
    plugin = Path(__file__).parent / "fixtures/c04-runtime-plugin/pyproject.toml"
    assert 'c08-stock = "fleet.c08_stock_linux_fixture:build_environment"' in plugin.read_text()


def test_wheel_inventory_keeps_complete_members_and_entrypoints(tmp_path):
    import zipfile

    builder = importlib.import_module(".build_c08_runner_image", __package__)
    wheel = tmp_path / "fixture-0.1-py3-none-any.whl"
    payloads = {
        "fixture/__init__.py": b"installed bytes\n",
        "fixture-0.1.dist-info/entry_points.txt": b"[deerflow.fleet.agent_environment]\nc08 = fixture:factory\n",
        "fixture-0.1.dist-info/RECORD": b"generated record\n",
    }
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, value in payloads.items():
            archive.writestr(name, value)
    inventory = builder.wheel_inventory(tmp_path)
    assert set(inventory) == {wheel.name}
    item = inventory[wheel.name]
    assert item["sha256"] == hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert item["members"] == {name: hashlib.sha256(value).hexdigest() for name, value in payloads.items()}
    assert item["entrypoints"] == {"deerflow.fleet.agent_environment": {"c08": "fixture:factory"}}


def test_prepared_context_is_unique_and_never_replaces_previous_inputs(tmp_path):
    import pytest

    builder = importlib.import_module(".build_c08_runner_image", __package__)
    context = builder.prepare_context(tmp_path / "original")
    original = (context / "prepared-inputs.json").read_bytes()
    with pytest.raises(FileExistsError):
        builder.prepare_context(context)
    assert (context / "prepared-inputs.json").read_bytes() == original


def test_continuation_observer_is_separate_installed_entrypoint(tmp_path):
    builder = importlib.import_module(".build_c08_runner_image", __package__)
    context = builder.prepare_context(tmp_path / "continuation-inputs")
    name = "c08_continuation_fixture.py"
    assert (context / name).read_bytes() == Path(__file__).with_name(name).read_bytes()
    assert name in (context / "Dockerfile").read_text()
    assert name in json.loads((context / "prepared-inputs.json").read_bytes())
    assert 'c08-continuation = "fleet.c08_continuation_fixture:build_environment"' in (Path(__file__).parent / "fixtures/c04-runtime-plugin/pyproject.toml").read_text()


def test_installed_member_check_rejects_changed_payload(tmp_path):
    import pytest

    from .c08_installed_bytes import verify_members

    path = tmp_path / "fixture/__init__.py"
    path.parent.mkdir()
    path.write_bytes(b"frozen")
    inventory = {"fixture.whl": {"members": {"fixture/__init__.py": hashlib.sha256(b"frozen").hexdigest(), "fixture.dist-info/RECORD": "generated"}}}
    assert verify_members(tmp_path, inventory) == 1
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="Installed wheel member differs"):
        verify_members(tmp_path, inventory)


def test_c07_variant_preserves_wheels_and_original_barrier_model(tmp_path):
    import pytest

    builder = importlib.import_module(".build_c08_runner_image", __package__)
    stock = builder.prepare_context(tmp_path / "stock")
    (stock / "original.whl").write_bytes(b"same frozen wheel")
    variant = builder.prepare_c07_variant(stock, tmp_path / "c07")
    assert (variant / "original.whl").read_bytes() == (stock / "original.whl").read_bytes()
    bundle = json.loads((variant / "runtime-bundle.json").read_bytes())
    assert bundle["plugins"] == [] and bundle["mcp_servers"] == {}
    assert bundle["skills"] == json.loads((stock / "runtime-bundle.json").read_bytes())["skills"]
    bindings = json.loads((variant / "model-bindings.json").read_bytes())
    assert bindings == {"model-1": {"provider_use": "fleet.c07_integration_fixture:BarrierModel", "target_model": "c07", "version": "v1"}}
    contracts = json.loads((variant / "workspace-contracts.json").read_bytes())
    assert contracts["plugins"] == [] and contracts["mcp_servers"] == {}
    with pytest.raises(FileExistsError):
        builder.prepare_c07_variant(stock, variant)
