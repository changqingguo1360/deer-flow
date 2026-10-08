"""Actual isolated PG migration without Fleet import or operator YAML."""

import asyncio
import os
import subprocess
import sys
from uuid import uuid4

import pytest
from sqlalchemy import text

from .test_c02_remote_agent_admission import admission as admission


@pytest.mark.asyncio
async def test_actual_pg_host_routing_migration_without_plugin_or_config(admission, tmp_path):
    engine, _, _, _ = admission
    schema = "c08_host_migration_" + uuid4().hex
    async with engine.begin() as connection:
        await connection.execute(text('CREATE SCHEMA "' + schema + '"'))
    script = r"""
import asyncio, os, sys
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine
from deerflow.config.app_config import AppConfig
from deerflow.persistence.bootstrap import _MIGRATIONS_DIR
try:
    AppConfig.resolve_config_path()
except FileNotFoundError:
    pass
else:
    raise AssertionError("Migration must not resolve operator configuration")
cfg = Config()
cfg.set_main_option("script_location", str(_MIGRATIONS_DIR))
cfg.set_main_option("sqlalchemy.url", os.environ["TEST_POSTGRES_URI"].replace("%", "%%"))
cfg.set_main_option("deerflow_pg_schema", sys.argv[1])
async def inspect_host(expected, insert=False, verify=False):
    engine = create_async_engine(os.environ["TEST_POSTGRES_URI"], connect_args={"server_settings": {"search_path": sys.argv[1]}})
    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT current_schema()")) == sys.argv[1]
            assert await connection.run_sync(lambda sync: inspect(sync).has_table("thread_execution_bindings")) is expected
            if expected:
                columns = await connection.run_sync(lambda sync: {column["name"] for column in inspect(sync).get_columns("thread_execution_bindings")})
                assert columns == {"user_id", "thread_id", "backend", "parent_thread_id", "source_workspace", "recovery_required"}
            if insert:
                await connection.execute(text("INSERT INTO thread_execution_bindings (user_id,thread_id,backend) VALUES ('owner','thread','fleet')"))
                assert await connection.scalar(text("SELECT recovery_required FROM thread_execution_bindings")) is False
            if verify:
                assert await connection.scalar(text("SELECT backend FROM thread_execution_bindings")) == "fleet"
    finally:
        await engine.dispose()
command.upgrade(cfg, "0017_personal_access_tokens")
asyncio.run(inspect_host(False))
command.upgrade(cfg, "head")
asyncio.run(inspect_host(True, insert=True))
command.upgrade(cfg, "head")
asyncio.run(inspect_host(True, verify=True))
command.downgrade(cfg, "0017_personal_access_tokens")
asyncio.run(inspect_host(False))
command.upgrade(cfg, "head")
asyncio.run(inspect_host(True))
assert not any(name.startswith("deerflow_ecs_fleet") for name in sys.modules)
"""
    env = dict(os.environ)
    env["DEER_FLOW_CONFIG_PATH"] = str(tmp_path / "absent-config.yaml")
    try:
        result = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", script, schema], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
    finally:
        async with engine.begin() as connection:
            await connection.execute(text('DROP SCHEMA "' + schema + '" CASCADE'))
