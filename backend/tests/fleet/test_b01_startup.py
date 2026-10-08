"""Fleet startup must preserve accepted work when new admission is closed."""

from types import SimpleNamespace

import pytest

from deerflow.config.app_config import AppConfig
from deerflow.extensions import reset_loaded_extensions, reset_runtime_diagnostics
from deerflow.extensions.loader import ExtensionLoadError, ExtensionSpec


@pytest.fixture(autouse=True)
def reset_extensions():
    reset_loaded_extensions()
    reset_runtime_diagnostics()
    yield
    reset_loaded_extensions()
    reset_runtime_diagnostics()


@pytest.mark.parametrize("ready,enabled,repository", [(False, True, True), (True, False, True), (True, True, False)])
def test_closed_admission_still_requires_ready_durable_tracking(ready, enabled, repository):
    from app.fleet.runtime import validate_fleet_task_runtime

    runtime = SimpleNamespace(fleet_protocol_version=1, ready=ready, config=SimpleNamespace(enabled=True, jobs_enabled=False))
    app = SimpleNamespace(state=SimpleNamespace(extensions=SimpleNamespace(services=(("fleet", runtime),))))
    with pytest.raises(RuntimeError, match="durable SQL"):
        validate_fleet_task_runtime(app, enabled=enabled, repository_available=repository)


@pytest.mark.parametrize("required,prefix", [(False, "fleet_"), (True, None), (True, "other_")])
def test_create_app_rejects_unsafe_enabled_fleet_before_loading(monkeypatch, required, prefix):
    import app.gateway.app as module

    config = AppConfig(sandbox={"use": "test"}, plugins=[ExtensionSpec(use="deerflow_ecs_fleet:install", required=required, table_prefix=prefix, config={"enabled": True})])
    monkeypatch.setattr(module, "get_app_config", lambda: config)
    with pytest.raises(RuntimeError, match="required.*fleet_"):
        module.create_app()


@pytest.mark.parametrize("private_enabled,host_enabled", [(False, True), (True, False)])
def test_disabled_fleet_retains_normal_optional_extension_behavior(monkeypatch, private_enabled, host_enabled):
    import app.gateway.app as module

    config = AppConfig(sandbox={"use": "test"}, plugins=[ExtensionSpec(use="deerflow_ecs_fleet:install", enabled=host_enabled, config={"enabled": private_enabled})])
    monkeypatch.setattr(module, "get_app_config", lambda: config)
    app = module.create_app()
    assert not app.state.extensions.services


def test_create_app_required_fleet_invalid_private_config_fails_closed(monkeypatch):
    import app.gateway.app as module

    config = AppConfig(
        sandbox={"use": "test"}, plugins=[ExtensionSpec(use="deerflow_ecs_fleet:install", required=True, table_prefix="fleet_", config={"enabled": True, "unknown_option": True, "nas_root": "/private/tmp/fleet", "nas_identity": "test"})]
    )
    monkeypatch.setattr(module, "get_app_config", lambda: config)
    with pytest.raises(ExtensionLoadError):
        module.create_app()


def test_enabled_gateway_fleet_requires_nas_when_admission_closed(monkeypatch):
    import app.gateway.app as module

    config = AppConfig(sandbox={"use": "test"}, plugins=[ExtensionSpec(use="deerflow_ecs_fleet:install", required=True, table_prefix="fleet_", config={"enabled": True, "jobs_enabled": False})])
    monkeypatch.setattr(module, "get_app_config", lambda: config)
    with pytest.raises(RuntimeError, match="NAS"):
        module.create_app()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_closed_admission_missing_mount_cannot_become_ready(fleet_database, tmp_path):
    from deerflow_extension_api import ExtensionRuntimeDeps

    from app.fleet.runtime import validate_fleet_task_runtime

    from .test_b02_fleet_foundation import service_class, settings

    _, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path).model_copy(update={"jobs_enabled": False}))
    (tmp_path / ".deerflow-fleet-root").unlink()
    with pytest.raises(FileNotFoundError):
        await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    assert not fleet.ready
    app = SimpleNamespace(state=SimpleNamespace(extensions=SimpleNamespace(services=(("fleet", fleet),))))
    with pytest.raises(RuntimeError, match="durable SQL"):
        validate_fleet_task_runtime(app, enabled=True, repository_available=True)
    await fleet.stop()
