"""The neutral routing migration runs without Fleet import or operator config."""

import os
import subprocess
import sys


def test_actual_plugin_absent_upgrade_from_previous_revision(tmp_path):
    script = r"""
import sys
from deerflow.config.app_config import AppConfig
try:
    AppConfig.resolve_config_path()
except FileNotFoundError:
    pass
else:
    raise AssertionError("Migration fixture must not resolve operator configuration")
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from deerflow.persistence.bootstrap import _MIGRATIONS_DIR
cfg = Config()
cfg.set_main_option("script_location", str(_MIGRATIONS_DIR))
cfg.set_main_option("sqlalchemy.url", "sqlite+aiosqlite:///" + sys.argv[1])
command.upgrade(cfg, "0017_personal_access_tokens")
engine = create_engine("sqlite:///" + sys.argv[1])
assert not inspect(engine).has_table("thread_execution_bindings")
command.upgrade(cfg, "head")
columns = {column["name"] for column in inspect(engine).get_columns("thread_execution_bindings")}
assert columns == {"user_id", "thread_id", "backend", "parent_thread_id", "source_workspace", "recovery_required"}
with engine.begin() as conn:
    conn.execute(text("INSERT INTO thread_execution_bindings (user_id,thread_id,backend) VALUES ('owner','thread','fleet')"))
    assert conn.execute(text("SELECT recovery_required FROM thread_execution_bindings")).scalar_one() == 0
command.upgrade(cfg, "head")
with engine.connect() as conn:
    assert conn.execute(text("SELECT backend FROM thread_execution_bindings")).scalar_one() == "fleet"
assert not any(name.startswith("deerflow_ecs_fleet") for name in sys.modules)
command.downgrade(cfg, "0017_personal_access_tokens")
assert not inspect(engine).has_table("thread_execution_bindings")
command.upgrade(cfg, "head")
assert inspect(engine).has_table("thread_execution_bindings")
engine.dispose()
"""
    env = dict(os.environ)
    env["DEER_FLOW_CONFIG_PATH"] = str(tmp_path / "absent-config.yaml")
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path / "old-host.db")], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
