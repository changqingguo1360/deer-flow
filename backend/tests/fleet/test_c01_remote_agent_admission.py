"""C01 versioned private launch storage and real Gateway prerequisite rejection."""

import importlib
import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from deerflow_extension_api import ExtensionRuntimeDeps
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from deerflow.config.app_config import AppConfig

from .test_b01_fleet_foundation import enabled_config
from .test_b02_fleet_foundation import service_class, settings

DIGEST = "sha256:" + "a" * 64


def c_config():
    raw = enabled_config(agents_enabled=True)
    raw["profiles"]["remote"] = {"kind": "agent", "image": DIGEST, "runtime_digest": DIGEST, "cpu_millis": 1000, "memory_mib": 2048}
    return raw


def test_agent_profiles_require_a_frozen_runtime_digest():
    from deerflow_ecs_fleet.config import FleetConfig

    raw = c_config()
    del raw["profiles"]["remote"]["runtime_digest"]
    with pytest.raises(ValidationError, match="runtime"):
        FleetConfig.model_validate(raw)


def host_config(**changes):
    data = {
        "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
        "memory": {"enabled": False},
        "database": {"backend": "postgres", "postgres_url": "postgresql://fixture@localhost/test", "postgres_schema": "c01"},
        "run_events": {"backend": "db"},
        "run_ownership": {"heartbeat_enabled": True},
        "plugins": [{"use": "deerflow_ecs_fleet:install", "required": True, "table_prefix": "fleet_", "config": c_config()}],
    }
    return AppConfig.model_validate(data | changes)


@pytest.mark.parametrize(
    "change",
    [
        {"run_events": {"backend": "memory"}},
        {"run_ownership": {"heartbeat_enabled": False}},
        {"checkpointer": {"type": "memory"}},
        {"checkpointer": {"type": "postgres", "connection_string": "postgresql://fixture@localhost/other", "postgres_schema": "c01"}},
    ],
)
def test_actual_gateway_rejects_incomplete_remote_configuration(monkeypatch, change):
    from app.gateway import app as module
    from deerflow.extensions import get_loaded_extensions, set_loaded_extensions

    cfg = host_config(**change)
    before = get_loaded_extensions()
    monkeypatch.setattr(module, "get_app_config", lambda: cfg)
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: cfg)
    try:
        with pytest.raises(RuntimeError, match="Remote Agent"):
            module.create_app()
    finally:
        set_loaded_extensions(before)


def implementation():
    assert importlib.util.find_spec("deerflow_ecs_fleet.launch_spec") is not None, "Versioned C launch specification not implemented"
    return importlib.import_module("deerflow_ecs_fleet.launch_spec")


def wire():
    return {
        "schema_version": 1,
        "run_id": "run-1",
        "agent_task_id": "task-1",
        "generation": 1,
        "user_id": "user-1",
        "thread_id": "thread-1",
        "assistant_id": "lead_agent",
        "profile": "remote",
        "model_name": "model-1",
        "model_version": "version-1",
        "input": {"messages": [{"role": "user", "content": "Untrusted secret-in-user-input"}]},
        "normalized_config": {"recursion_limit": 123, "configurable": {"thread_id": "thread-1"}, "context": {"user_id": "user-1", "model_name": "model-1"}, "metadata": {"custom": {"keep": [1, 2]}}},
        "stream_modes": ["values", "messages-tuple", "custom"],
        "stream_subgraphs": True,
        "interrupt_before": ["tools"],
        "interrupt_after": "*",
        "recursion_limit": 123,
        "execution_deadline": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        "runtime_digest": DIGEST,
        "skill_snapshot": {"entries": [{"name": "skill-1", "version": "v1", "digest": DIGEST}]},
        "plugin_snapshot": {"entries": [{"name": "plugin-1", "version": "v1", "digest": DIGEST}]},
        "workspace_manifest_ref": "workspace-1",
        "secret_refs": [{"name": "MODEL_API_KEY", "reference_id": "model-key-v1"}],
        "resources": {"cpu_millis": 1000, "memory_mib": 2048, "agent_units": 1},
    }


def test_launch_spec_roundtrip_freezes_input_and_redacts_public_summary():
    module = implementation()
    raw = wire()
    spec = module.LaunchSpec.model_validate(raw)
    rebuilt = module.LaunchSpec.model_validate_json(spec.model_dump_json())
    assert rebuilt.model_dump(mode="json") == spec.model_dump(mode="json")
    raw["normalized_config"]["metadata"]["custom"]["keep"].append(999)
    assert rebuilt.model_dump(mode="json")["normalized_config"]["metadata"]["custom"]["keep"] == [1, 2]
    with pytest.raises((TypeError, ValidationError)):
        rebuilt.normalized_config["metadata"]["custom"]["keep"] += (3,)
    summary = json.dumps(rebuilt.public_summary())
    for private in ("secret-in-user-input", "MODEL_API_KEY", "model-key-v1", "custom", "plugin-1", "workspace-1"):
        assert private not in summary
    assert rebuilt.public_summary()["recursion_limit"] == 123


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": 2},
        {"schema_version": True},
        {"generation": 0},
        {"generation": True},
        {"run_id": "../escape"},
        {"stream_modes": ["invented"]},
        {"recursion_limit": 124},
        {"execution_deadline": "2030-01-01T00:00:00"},
        {"workspace_manifest_ref": "../data"},
        {"secret_refs": [{"name": "API_KEY", "reference_id": "https://user:password@host"}]},
        {"normalized_config": {"recursion_limit": 123, "configurable": {"thread_id": "thread-1"}, "context": {"api_key": "sk-private-credential"}}},
    ],
)
def test_launch_spec_rejects_invalid_or_secret_bearing_execution_fields(change):
    module = implementation()
    with pytest.raises(ValidationError):
        module.LaunchSpec.model_validate(wire() | change)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_c01_contract(fleet_database, tmp_path):
    module = implementation()
    from deerflow_ecs_fleet.persistence.agent_tasks import AgentTasks
    from deerflow_ecs_fleet.persistence.placements import RunPlacements

    engine, sf, _ = fleet_database
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    spec = module.LaunchSpec.model_validate(wire())
    tasks, placements = AgentTasks(), RunPlacements()
    try:
        async with sf.begin() as session:
            await tasks.create(session, task_id=spec.agent_task_id, user_id=spec.user_id, thread_id=spec.thread_id, deadline=spec.execution_deadline, continuation_budget=3)
            await placements.create(session, spec=spec, queue_deadline=spec.execution_deadline)
        await fleet.stop()
        await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
        async with sf() as session:
            loaded = await placements.load_launch_spec(session, run_id=spec.run_id, user_id=spec.user_id, thread_id=spec.thread_id)
            assert loaded.model_dump(mode="json") == spec.model_dump(mode="json")
            advertised = module.WorkerCompatibility(runtime_digest=DIGEST, skill_snapshot=spec.skill_snapshot, plugin_snapshot=spec.plugin_snapshot)
            assert (await placements.compatible_launch(session, run_id=spec.run_id, user_id=spec.user_id, thread_id=spec.thread_id, worker=advertised)).run_id == spec.run_id
            for field in ("runtime_digest", "skill_snapshot", "plugin_snapshot"):
                payload = advertised.model_dump(mode="json")
                if field == "runtime_digest":
                    payload[field] = "sha256:" + "b" * 64
                else:
                    payload[field]["entries"][0]["digest"] = "sha256:" + "b" * 64
                with pytest.raises(ValueError, match="incompatible"):
                    await placements.compatible_launch(session, run_id=spec.run_id, user_id=spec.user_id, thread_id=spec.thread_id, worker=module.WorkerCompatibility.model_validate(payload))
            with pytest.raises(LookupError):
                await placements.load_launch_spec(session, run_id=spec.run_id, user_id="other", thread_id=spec.thread_id)
            public = await placements.public_summary(session, run_id=spec.run_id, user_id=spec.user_id, thread_id=spec.thread_id)
            task_summary = await tasks.public_summary(session, task_id=spec.agent_task_id, user_id=spec.user_id, thread_id=spec.thread_id)
            assert "secret" not in json.dumps(public | task_summary).lower()
        for command in ('UPDATE fleet_launch_specs SET payload=payload || \'{"run_id":"forged"}\'::jsonb', "DELETE FROM fleet_launch_specs"):
            with pytest.raises(DBAPIError):
                async with engine.begin() as conn:
                    await conn.execute(text(command))
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT count(*) FROM fleet_launch_specs"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM fleet_run_placements"))).scalar_one() == 1
            assert (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one() == 0
        with pytest.raises(IntegrityError):
            async with sf.begin() as session:
                await tasks.create(session, task_id="task-2", user_id=spec.user_id, thread_id=spec.thread_id, deadline=spec.execution_deadline, continuation_budget=3)
        rollback_spec = module.LaunchSpec.model_validate(
            wire()
            | {"execution_deadline": spec.execution_deadline, "run_id": "rolled-run", "agent_task_id": "rolled-back", "thread_id": "other-thread", "normalized_config": {"recursion_limit": 123, "configurable": {"thread_id": "other-thread"}}}
        )
        async with sf.begin() as session:
            await tasks.create(session, task_id="rolled-back", user_id=spec.user_id, thread_id="other-thread", deadline=spec.execution_deadline, continuation_budget=3)
            await placements.create(session, spec=rollback_spec, queue_deadline=spec.execution_deadline)
            await session.rollback()
        async with engine.connect() as conn:
            for table in ("fleet_agent_tasks", "fleet_run_placements", "fleet_launch_specs"):
                assert (await conn.execute(text("SELECT count(*) FROM " + table))).scalar_one() == 1
    finally:
        await fleet.stop()


@pytest.mark.parametrize("model", ["qwen3:32b", "google/gemini-2.5-pro-preview"])
def test_existing_operator_model_identifiers_and_package_versions_are_valid(model):
    module = implementation()
    raw = wire() | {"model_name": model, "model_version": "2026.10.1+operator.1"}
    raw["plugin_snapshot"]["entries"][0]["version"] = "1.0.0+local.1"
    spec = module.LaunchSpec.model_validate(raw)
    assert spec.model_name == model
    assert spec.plugin_snapshot.entries[0].version == "1.0.0+local.1"


def test_actual_host_unified_postgres_is_valid_but_gateway_runner_remains_closed(monkeypatch):
    from app.fleet.runtime import validate_remote_agent_host_configuration
    from app.gateway import app as module
    from deerflow.extensions import get_loaded_extensions, set_loaded_extensions

    cfg = host_config()
    assert cfg.checkpointer is None
    validate_remote_agent_host_configuration(cfg)
    before = get_loaded_extensions()
    monkeypatch.setattr(module, "get_app_config", lambda: cfg)
    try:
        with pytest.raises(RuntimeError, match="runner and fenced persistence"):
            module.create_app()
    finally:
        set_loaded_extensions(before)


def test_operator_builder_preserves_full_run_parameters_and_job_wire_v1(tmp_path):
    from deerflow_ecs_fleet.config import FleetConfig

    module = implementation()
    cfg = FleetConfig.model_validate(c_config())
    params = wire()
    for field in ("profile", "runtime_digest", "resources"):
        params.pop(field)
    spec = module.build_launch_spec(profile_name="remote", profile=cfg.profiles["remote"], run_parameters=params)
    assert spec.stream_modes == ("values", "messages-tuple", "custom")
    assert spec.interrupt_before == ("tools",) and spec.interrupt_after == "*"
    assert spec.resources.memory_mib == 2048
    with pytest.raises(ValueError, match="Operator"):
        module.build_launch_spec(profile_name="remote", profile=cfg.profiles["remote"], run_parameters=params | {"resources": {}})
    job = settings(tmp_path).profiles["batch"]
    assert "runtime_digest" not in job.job_wire()
    assert job.job_wire() == job.model_dump(exclude={"runtime_digest"})


@pytest.mark.integration
@pytest.mark.asyncio
async def test_f0006_upgrade_preserves_b_records(fleet_database, tmp_path):
    from alembic import command
    from alembic.config import Config
    from deerflow_ecs_fleet import service

    engine, sf, _ = fleet_database

    def previous(sync):
        config = Config()
        config.set_main_option("script_location", str(Path(service.__file__).parent / "migrations"))
        config.attributes["connection"] = sync
        command.upgrade(config, "f0006_nodes")

    async with engine.begin() as conn:
        await conn.run_sync(previous)
        await conn.execute(text("INSERT INTO fleet_nodes (id,name,cpu_millis,memory_mib,profile_allowlist,registered_by) VALUES ('existing','existing',1000,512,'[\"batch\"]','operator')"))
        await conn.execute(text("INSERT INTO fleet_credentials (id,node_id,token_hash,expires_at) VALUES ('credential','existing','hash',clock_timestamp()+interval '1 hour')"))
        await conn.execute(
            text(
                "INSERT INTO fleet_jobs (id,user_id,thread_id,tracking_task_id,idempotency_key,spec,state,queue_deadline,queued_at) VALUES ('existing-job','u','t','tracking','key','{}','queued',"
                "clock_timestamp()+interval '1 hour',clock_timestamp())"
            )
        )
        before = {table: [dict(row) for row in (await conn.execute(text("SELECT * FROM " + table))).mappings()] for table in ("fleet_nodes", "fleet_credentials", "fleet_jobs")}
    fleet = service_class()(settings(tmp_path))
    await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
    try:
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT version_num FROM fleet_alembic_version"))).scalar_one() == "f0009_workspace_points"
            for table, expected in before.items():
                assert [dict(row) for row in (await conn.execute(text("SELECT * FROM " + table))).mappings()] == expected
            for table in ("fleet_agent_tasks", "fleet_launch_specs", "fleet_run_placements", "fleet_event_outbox", "fleet_stream_seals"):
                assert (await conn.execute(text("SELECT count(*) FROM " + table))).scalar_one() == 0
    finally:
        await fleet.stop()
