"""Legal B-only empty profiles retain a finite workspace read budget."""

import os
from dataclasses import replace
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment
from .test_c08_terminal_pair import prepared_pair as prepared_pair


@pytest.mark.asyncio
async def test_required_b_only_empty_profiles_installs_original_ownership(fleet_database, tmp_path):
    from deerflow_ecs_fleet.config import ExecutionProfile
    from deerflow_extension_api import ExtensionRuntimeDeps

    from app.fleet.ownership import install_fleet_ownership
    from app.fleet.runtime import validate_fleet_plugin_configuration
    from deerflow.config.app_config import AppConfig
    from deerflow.extensions.loader import ExtensionSpec
    from deerflow.persistence.base import Base
    from deerflow.persistence.run import RunRepository

    from .test_b02_fleet_foundation import service_class, settings

    engine, sf, _ = fleet_database
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    config = settings(tmp_path).model_copy(update={"jobs_enabled": False, "profiles": {}})
    assert config.enabled and not config.jobs_enabled and not config.agents_enabled
    host = AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "database": {"backend": "postgres", "postgres_url": engine.url.render_as_string(hide_password=False)},
            "mcp_tasks": {"enabled": True},
            "plugins": [ExtensionSpec(use="deerflow_ecs_fleet:install", required=True, table_prefix="fleet_", config=config.model_dump(mode="json"))],
        }
    )
    validate_fleet_plugin_configuration(host.plugins, host_config=host)
    fleet = service_class()(config)
    try:
        await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
        app = SimpleNamespace(state=SimpleNamespace(extensions=SimpleNamespace(services=(("fleet", fleet),)), run_store=RunRepository(sf)))
        install_fleet_ownership(app, sf)
        assert app.state.fleet_workspace_files.versions.max_output_bytes == ExecutionProfile.model_fields["max_output_bytes"].default == 64 * 1024 * 1024
        async with engine.connect() as conn:
            assert await conn.scalar(text("SELECT count(*) FROM fleet_attempts")) == 0
    finally:
        await fleet.stop()


def test_nonempty_profile_maximum_is_preserved(tmp_path):
    from app.fleet.workspace_files import FleetWorkspaceFiles

    from .test_b02_fleet_foundation import settings

    config = settings(tmp_path)
    profile = config.profiles["batch"]
    config = config.model_copy(update={"profiles": {"small": profile.model_copy(update={"max_output_bytes": 4096}), "large": profile.model_copy(update={"max_output_bytes": 8192})}})
    assert FleetWorkspaceFiles(None, config).versions.max_output_bytes == 8192


def test_c_enabled_empty_profiles_remain_invalid(tmp_path):
    from deerflow_ecs_fleet.config import FleetConfig

    from .test_b02_fleet_foundation import settings

    payload = settings(tmp_path).model_dump() | {"agents_enabled": True, "profiles": {}}
    with pytest.raises(ValidationError):
        FleetConfig.model_validate(payload)


@pytest.mark.asyncio
async def test_empty_profile_default_reads_bounded_sealed_output_and_rejects_overflow(prepared_pair, tmp_path):
    from app.fleet.workspace_files import FleetWorkspaceFiles

    from .test_b02_fleet_foundation import settings

    p = prepared_pair
    config = settings(tmp_path).model_copy(update={"jobs_enabled": False, "profiles": {}, "nas_root": p.versions.nas.root, "nas_identity": "task4"})
    versions = FleetWorkspaceFiles(p.sf, config).versions
    source = tmp_path / "bounded-source"
    source.mkdir()
    for category in ("workspace", "uploads", "outputs"):
        (source / category).mkdir()
    output = source / "outputs" / "limit.bin"
    with output.open("wb") as stream:
        stream.truncate(versions.max_output_bytes)
    fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        manifest = versions.seal(replace(p.identity, request_id="bounded-output", publication_key="bounded-output"), fd)
        stream, item = versions.open_verified_output(manifest, "outputs/limit.bin")
        try:
            assert item.size == versions.max_output_bytes
            assert len(stream.read()) == versions.max_output_bytes
        finally:
            stream.close()
        with output.open("r+b") as stream:
            stream.truncate(versions.max_output_bytes + 1)
        with pytest.raises(ValueError, match="byte limit"):
            versions.seal(replace(p.identity, request_id="overflow-output", publication_key="overflow-output"), fd)
    finally:
        os.close(fd)
