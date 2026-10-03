"""Real packaged extension installation and host migration isolation."""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from sqlalchemy import text

from .test_b02_fleet_foundation import service_class, settings


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_manager_installs_fleet_snapshot_into_isolated_checkout(tmp_path, fleet_database, monkeypatch):
    from deerflow.extensions.manager import ExtensionManager

    _, _, schema = fleet_database
    source_backend = Path(__file__).resolve().parents[2]
    project = tmp_path / "checkout"
    backend = project / "backend"
    backend.mkdir(parents=True)
    ignores = shutil.ignore_patterns(".venv", "__pycache__", "*.pyc", ".git")
    for name in ("pyproject.toml", "uv.lock", "README.md"):
        shutil.copy2(source_backend / name, backend / name)
    for name in ("harness", "extension-api"):
        shutil.copytree(source_backend / "packages" / name, backend / "packages" / name, ignore=ignores)
    shutil.copytree(source_backend / "app", backend / "app", ignore=ignores)
    import yaml

    (project / "scripts").mkdir()
    shutil.copy2(source_backend.parent / "scripts" / "detect_uv_extras.py", project / "scripts" / "detect_uv_extras.py")
    monkeypatch.setenv("FLEET_INSTALL_TEST_POSTGRES_URI", os.environ["TEST_POSTGRES_URI"])
    config_document = {
        "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
        "memory": {"enabled": False},
        "title": {"enabled": False},
        "database": {"backend": "postgres", "postgres_url": "$FLEET_INSTALL_TEST_POSTGRES_URI", "postgres_schema": schema},
        "mcp_tasks": {"enabled": True},
        "plugins": [],
    }
    (project / "config.yaml").write_text(yaml.safe_dump(config_document))
    manager = ExtensionManager(project)
    installed = manager.install(str(source_backend / "packages" / "ecs-fleet"), yes=True, required=True)
    assert installed.use == "deerflow_ecs_fleet:install"
    plugin = yaml.safe_load((project / "config.yaml").read_text())["plugins"][0]
    assert plugin["required"] is True
    plugin["enabled"] = False
    config_document["plugins"] = [plugin]
    (project / "config.yaml").write_text(yaml.safe_dump(config_document))
    snapshot = backend / "extensions" / "sources" / "deerflow-ecs-fleet"
    assert snapshot.is_dir()
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env["DEER_FLOW_HOME"] = str(tmp_path / "home")
    (project / "extensions_config.json").write_text('{"mcpServers": {}, "skills": {}}')
    env["DEER_FLOW_EXTENSIONS_CONFIG_PATH"] = str(project / "extensions_config.json")
    env["FLEET_INSTALL_TEST_SCHEMA"] = schema
    env["FLEET_INSTALL_TEST_NAS"] = str(tmp_path / "nas")
    env["DEER_FLOW_CONFIG_PATH"] = str(project / "config.yaml")
    # Resolve from the synchronized environment, never the Fleet test source path.
    probe = """
import importlib.metadata as m, json, sys
from pathlib import Path
from app.gateway.app import create_app
app = create_app()
assert not app.state.extensions.services
assert not any(name.startswith('deerflow_ecs_fleet') for name in sys.modules)
import deerflow_ecs_fleet as fleet
entry = list(m.distribution('deerflow-ecs-fleet').entry_points)
assert len(entry) == 1 and entry[0].value == 'deerflow_ecs_fleet:install'
path = Path(fleet.__file__).resolve()
assert 'site-packages' in str(path), path
assert len(list((path.parent / 'migrations' / 'versions').glob('*.py'))) == 8
for name in list(sys.modules):
    if name.startswith('deerflow_ecs_fleet'):
        del sys.modules[name]
from app.gateway.app import create_app
app = create_app()
assert not app.state.extensions.services
assert not any(name.startswith('deerflow_ecs_fleet') for name in sys.modules)
import asyncio, os, yaml
from sqlalchemy import text
from deerflow.config.app_config import reload_app_config
from deerflow.mcp.tasks.fleet_runtime import is_fleet_job_runtime_available
async def migrate_installed_artifact():
    nas = Path(os.environ['FLEET_INSTALL_TEST_NAS'])
    nas.mkdir()
    (nas / '.deerflow-fleet-root').write_text('installation-test\\n')
    config_file = Path(os.environ['DEER_FLOW_CONFIG_PATH'])
    config = yaml.safe_load(config_file.read_text())
    config['plugins'][0].update(enabled=True, table_prefix='fleet_', config={'enabled': True, 'jobs_enabled': False, 'nas_root': str(nas), 'nas_identity': 'installation-test'})
    config_file.write_text(yaml.safe_dump(config))
    reload_app_config()
    for cycle in range(2):
        app = create_app()
        async with app.router.lifespan_context(app):
            fleet = next(service for _, service in app.state.extensions.services if getattr(service, 'fleet_protocol_version', None) == 1)
            assert fleet.ready and fleet.jobs is not None
            assert app.state.mcp_tasks_available
            assert app.state.mcp_task_repo is not None
            assert app.state.mcp_task_service.drivers.get('fleet') is not None
            assert not is_fleet_job_runtime_available()
            assert await app.state.mcp_task_repo.list_by_thread('installation-test', user_id='installation-test') == []
            from deerflow.tools.tools import get_available_tools
            from deerflow.config.app_config import get_app_config
            for delegated in (False, True):
                assert 'submit_fleet_job' not in {tool.name for tool in get_available_tools(include_mcp=False, app_config=get_app_config(), subagent_enabled=delegated)}
            async with fleet.session_factory() as session:
                assert (await session.execute(text('SELECT current_schema()'))).scalar_one() == os.environ['FLEET_INSTALL_TEST_SCHEMA']
                assert (await session.execute(text('SELECT version_num FROM fleet_alembic_version'))).scalar_one() == 'f0008_event_outbox'
                assert (await session.execute(text("SELECT to_regclass('mcp_tasks')"))).scalar_one() == 'mcp_tasks'
                if cycle == 0:
                    await session.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib) VALUES ('installed-node','installed-node',1000,512)"))
                    await session.commit()
                else:
                    assert (await session.execute(text("SELECT name FROM fleet_nodes WHERE id='installed-node'"))).scalar_one() == 'installed-node'
        assert not fleet.ready and fleet.jobs is None
asyncio.run(migrate_installed_artifact())
print(json.dumps({'distribution': m.version('deerflow-ecs-fleet'), 'path': str(path), 'migration': 'f0008_event_outbox', 'restart': True}))
"""
    result = subprocess.run([str(backend / ".venv" / "bin" / "python"), "-c", probe], cwd=backend, env=env, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    evidence = json.loads(result.stdout.splitlines()[-1])
    assert evidence["distribution"] == "0.1.0"
    assert evidence["migration"] == "f0008_event_outbox"
    assert evidence["restart"] is True
    # Remove only this test's installed package. Required missing-package boot
    # must fail through the real loader; the managed source snapshot must not
    # accidentally make it importable.
    package = Path(evidence["path"]).parent
    absent = package.with_name("fleet_test_removed_package")
    package.rename(absent)
    (project / "config.yaml").write_text(yaml.safe_dump({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "plugins": []}))
    default_probe = """
import importlib.util, sys
assert importlib.util.find_spec('deerflow_ecs_fleet') is None
from app.gateway.app import create_app
app = create_app()
assert not app.state.extensions.services
assert not any(name.startswith('deerflow_ecs_fleet') for name in sys.modules)
print('default-without-fleet-package')
"""
    default = subprocess.run([str(backend / ".venv" / "bin" / "python"), "-c", default_probe], cwd=backend, env=env, capture_output=True, text=True, timeout=120)
    assert default.returncode == 0, default.stderr
    assert default.stdout.strip() == "default-without-fleet-package"
    plugin.update(enabled=True, table_prefix="fleet_", config={"enabled": True, "nas_root": str(tmp_path / "nas"), "nas_identity": "installation-test"})
    (project / "config.yaml").write_text(yaml.safe_dump({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "plugins": [plugin]}))
    missing_probe = """
import importlib.util
from deerflow.extensions.loader import ExtensionLoadError
assert importlib.util.find_spec('deerflow_ecs_fleet') is None
try:
    from app.gateway.app import create_app
except ExtensionLoadError:
    print('required-missing-package-rejected')
else:
    raise AssertionError('Gateway silently ignored missing required Fleet')
"""
    try:
        missing = subprocess.run([str(backend / ".venv" / "bin" / "python"), "-c", missing_probe], cwd=backend, env=env, capture_output=True, text=True, timeout=120)
        assert missing.returncode == 0, missing.stderr
        assert missing.stdout.strip() == "required-missing-package-rejected"
    finally:
        absent.rename(package)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_actual_postgres_autogenerate_preserves_private_fleet_tables(fleet_database, tmp_path):
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    import deerflow.persistence.models  # noqa: F401
    from deerflow.persistence.base import Base
    from deerflow.persistence.migrations import _env_filters as filters

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    config = tmp_path / "config.yaml"
    config.write_text("plugins:\n  - use: deerflow_ecs_fleet:install\n    required: true\n    table_prefix: fleet_\n    config: {enabled: false}\n")
    previous = filters.EXTENSION_TABLE_PREFIXES.copy()
    try:
        filters.EXTENSION_TABLE_PREFIXES.clear()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(text("CREATE TABLE host_autogen_probe (id INTEGER PRIMARY KEY)"))

            def diff(connection):
                context = MigrationContext.configure(connection, opts={"include_object": filters.include_object})
                return compare_metadata(context, Base.metadata)

            before = await conn.run_sync(diff)
            assert any(op[0] == "remove_table" and op[1].name == "fleet_jobs" for op in before)
            assert filters.register_configured_extension_table_prefixes(str(config)) == ("fleet_",)
            after = await conn.run_sync(diff)
            removed = [op[1].name for op in after if op[0] == "remove_table"]
            assert "host_autogen_probe" in removed
            assert not any(name.startswith("fleet_") for name in removed)
    finally:
        filters.EXTENSION_TABLE_PREFIXES.clear()
        filters.EXTENSION_TABLE_PREFIXES.update(previous)
        await fleet.stop()
