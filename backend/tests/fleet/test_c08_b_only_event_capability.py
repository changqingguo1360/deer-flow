"""Real legal B-only startup storage and event installation; no physical Runner proof."""

import json
import os
from contextlib import AsyncExitStack
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import event, text

from .test_c08_finishing_stream_reader import admission as admission
from .test_c08_finishing_stream_reader import checkpoint_owner as checkpoint_owner
from .test_c08_finishing_stream_reader import owner_environment as owner_environment
from .test_c08_finishing_stream_reader import prepared_pair as prepared_pair


@pytest.mark.asyncio
@pytest.mark.parametrize("checkpoint_type", ["memory", "sqlite"])
async def test_b_only_legacy_checkpoint_does_not_install_c_publication(fleet_database, tmp_path, checkpoint_type):
    from deerflow_extension_api import ExtensionRuntimeDeps

    from app.fleet.events import FleetGatewayBridge, install_fleet_events
    from app.fleet.runtime import validate_fleet_plugin_configuration, validate_fleet_task_runtime
    from deerflow.config.app_config import AppConfig
    from deerflow.extensions.loader import ExtensionSpec
    from deerflow.persistence.base import Base
    from deerflow.persistence.mcp_tasks import McpTaskRepository
    from deerflow.persistence.models.run_event import RunEventRow
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.checkpointer.provider import _resolve_checkpointer_config
    from deerflow.runtime.stream_bridge.base import END_SENTINEL
    from deerflow.runtime.stream_bridge.redis import RedisStreamBridge

    from .c07_integration_fixture import owned_redis
    from .test_b02_fleet_foundation import service_class, settings

    engine, sf, schema = fleet_database
    assert RunEventRow.__table__.metadata is Base.metadata
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    fleet_config = settings(tmp_path)
    assert fleet_config.enabled and fleet_config.jobs_enabled and not fleet_config.agents_enabled
    checkpoint = {"type": checkpoint_type}
    if checkpoint_type == "sqlite":
        checkpoint["connection_string"] = str(tmp_path / "legacy-checkpoint.sqlite")
    config = AppConfig.model_validate(
        {
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "database": {"backend": "postgres", "postgres_url": engine.url.render_as_string(hide_password=False), "postgres_schema": schema},
            "checkpointer": checkpoint,
            "mcp_tasks": {"enabled": True},
            "plugins": [ExtensionSpec(use="deerflow_ecs_fleet:install", required=True, table_prefix="fleet_", config=fleet_config.model_dump(mode="json"))],
        }
    )
    validate_fleet_plugin_configuration(config.plugins, host_config=config)
    assert _resolve_checkpointer_config(config).type == checkpoint_type
    statements = []

    def observe(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    async with AsyncExitStack() as resources:
        saver = await resources.enter_async_context(make_checkpointer(config))
        assert saver is not None
        fleet = service_class()(fleet_config)
        resources.push_async_callback(fleet.stop)
        await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
        repository = McpTaskRepository(sf)
        app = SimpleNamespace(state=SimpleNamespace(extensions=SimpleNamespace(services=(("fleet", fleet),)), mcp_tasks=repository))
        validate_fleet_task_runtime(app, enabled=config.mcp_tasks.enabled, repository_available=True)
        async with engine.connect() as conn:
            checkpoint_table = (await conn.execute(text("SELECT to_regclass('checkpoints')"))).scalar_one()
            placement_count = (await conn.execute(text("SELECT count(*) FROM fleet_run_placements"))).scalar_one()
            assert checkpoint_table is None and placement_count == 0
        redis, port, _ = await resources.enter_async_context(owned_redis(tmp_path / "redis"))
        local = RedisStreamBridge(redis_url=f"redis://127.0.0.1:{port}/0", client=redis, key_prefix="bonly:" + schema)
        app.state.stream_bridge = local
        bridge = install_fleet_events(app, sf)
        resources.push_async_callback(bridge.close)
        assert isinstance(bridge, FleetGatewayBridge) and bridge.local_bridge is local
        event.listen(engine.sync_engine, "before_cursor_execute", observe)
        resources.callback(event.remove, engine.sync_engine, "before_cursor_execute", observe)
        failure = None
        if bridge.publisher is not None:
            try:
                await bridge.publisher.publish_once()  # actual pre-fix installed C publisher, real PG parse
            except Exception as error:
                failure = error
        proof = {
            "checkpoint_type": checkpoint_type,
            "application_checkpoints_absent": checkpoint_table is None,
            "remote_placements": placement_count,
            "publisher_installed": bridge.publisher is not None,
            "actual_query_statements": statements,
            "publish_error_type": type(failure).__name__ if failure else None,
        }
        directory = os.environ.get("C08_B_ONLY_EVIDENCE_DIR")
        if directory:
            target = Path(directory) / (checkpoint_type + ".json")
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("x") as stream:
                json.dump(proof, stream, indent=2)
        assert failure is None, "Legal B-only zero-C-row installed publisher queried missing application checkpoints"
        assert bridge.publisher is None and bridge._publisher_redis is None
        await bridge.start()
        await bridge.publish("local-b-run", "messages", {"text": "local B event"})
        await bridge.publish_end("local-b-run")
        items = [item async for item in bridge.subscribe("local-b-run")]
        assert items[-1] is END_SENTINEL and items[0].data == {"text": "local B event"}
        assert not any("checkpoints" in sql.lower() for sql in statements)
        assert not any(sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for sql in statements)
        async with engine.connect() as conn:
            attempt_count = (await conn.execute(text("SELECT count(*) FROM fleet_attempts"))).scalar_one()
            checkpoint_after = (await conn.execute(text("SELECT to_regclass('checkpoints')"))).scalar_one()
        assert attempt_count == 0 and checkpoint_after is None
        if directory:
            final = {
                "checkpoint_type": checkpoint_type,
                "publisher_installed": bridge.publisher is not None,
                "actual_query_statements_after_local_actions": list(statements),
                "local_frames": [{"event": frame.event, "data": frame.data} for frame in items[:-1]],
                "observed_END": items[-1] is END_SENTINEL,
                "actual_attempt_count": attempt_count,
                "actual_checkpoint_table_after": checkpoint_after,
            }
            with (Path(directory) / (checkpoint_type + "-final.json")).open("x") as stream:
                json.dump(final, stream, indent=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutated", [False, True])
@pytest.mark.parametrize("agents_enabled", [False, True])
async def test_c_disabled_history_keeps_exact_reader_instead_of_local_fallback(prepared_pair, tmp_path, mutated, agents_enabled):
    from deerflow_extension_api import ExtensionRuntimeDeps

    from app.fleet.events import FleetGatewayBridge, install_fleet_events
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    from .test_b02_fleet_foundation import service_class, settings
    from .test_c08_finishing_stream_reader import finishing_stream

    pair = prepared_pair
    capability, identity, producer, record, _ = await finishing_stream(pair)
    with remote_mutation_scope(capability.context):
        await producer.publish_end(identity.run_id)
    fleet_config = settings(tmp_path)
    if agents_enabled:
        from deerflow_ecs_fleet.config import FleetConfig

        from .test_c01_remote_agent_admission import c_config

        data = c_config() | {"nas_root": str(fleet_config.nas_root), "nas_identity": fleet_config.nas_identity}
        fleet_config = FleetConfig.model_validate(data)
    fleet = service_class()(fleet_config)
    try:
        await fleet.start(ExtensionRuntimeDeps(session_factory=pair.sf))
        local = MemoryStreamBridge()
        app = SimpleNamespace(state=SimpleNamespace(extensions=SimpleNamespace(services=(("fleet", fleet),)), stream_bridge=local))
        bridge = install_fleet_events(app, pair.sf)
        try:
            assert isinstance(bridge, FleetGatewayBridge)
            assert (bridge.publisher is not None) == agents_enabled
            if agents_enabled:
                assert bridge.publisher.candidate_pointers.__self__ is bridge.reader
                assert bridge.publisher.recover_seals.__self__.sf is pair.sf
                assert await bridge.publisher.recover_seals() == 0
                async with pair.sf() as session:
                    candidates = await bridge.publisher.candidate_pointers(session, limit=64)
                assert len(candidates) == 1 and candidates[0].run_id == identity.run_id
            assert await bridge.is_remote(identity.run_id)
            assert await bridge.reader.identity(identity.run_id) == identity
            prepared = await bridge.reader.prepare(record, None)
            prior = await bridge.reader.page(prepared)
            assert len(prior) == 1 and await bridge.reader.seal(identity) is not None
            if mutated:
                async with pair.item.engine.begin() as conn:
                    await conn.execute(text("UPDATE fleet_agent_tasks SET accepted_workspace_point_id=NULL"))
                assert await bridge.reader.identity(identity.run_id) is None
                assert await bridge.reader.ensure_history_available(prepared, expected_seq=prior[0]["seq"]) is False
                assert await bridge.reader.page(prepared) is None and await bridge.reader.seal(identity) is None
            else:
                assert prior[0]["content"]["data"] == {"tail": "actual-before-finishing"}
            with pytest.raises(Exception, match="original execution adapter"):
                await bridge.publish(identity.run_id, "values", {"unauthorized": True})
        finally:
            await bridge.close()
    finally:
        await fleet.stop()


@pytest.mark.asyncio
async def test_c_disabled_remote_without_checkpoint_schema_never_falls_back_local(owner_environment, tmp_path):
    """Original C admission SQL mapping in a schema without checkpoint setup; no physical proof."""
    from deerflow_extension_api import ExtensionRuntimeDeps
    from sqlalchemy.exc import DBAPIError

    from app.fleet.events import install_fleet_events
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    from .test_b02_fleet_foundation import service_class, settings

    engine, sf, _, _, original_app, record, *_ = owner_environment
    async with engine.connect() as conn:
        assert (await conn.execute(text("SELECT to_regclass('checkpoints')"))).scalar_one() is None
    fleet = service_class()(settings(tmp_path))
    try:
        await fleet.start(ExtensionRuntimeDeps(session_factory=sf))
        local = MemoryStreamBridge()
        app = SimpleNamespace(state=SimpleNamespace(extensions=SimpleNamespace(services=(("fleet", fleet),)), stream_bridge=local))
        bridge = install_fleet_events(app, sf)
        try:
            assert bridge.publisher is None and await bridge.is_remote(record.run_id)
            with pytest.raises(DBAPIError, match="checkpoints"):
                await bridge.prepare(record, None)
            with pytest.raises(Exception, match="original execution adapter"):
                await bridge.publish(record.run_id, "values", {"unauthorized": True})
        finally:
            await bridge.close()
    finally:
        await fleet.stop()
