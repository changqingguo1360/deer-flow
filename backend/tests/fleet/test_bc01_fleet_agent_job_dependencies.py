"""BC01 real migrations, original execution fence and durable child ownership."""

import inspect
import json
import os
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text

from .conftest import fleet_database as base_fleet_database
from .test_c02_remote_agent_admission import admission as admission
from .test_c03_remote_agent_admission import owner_environment as owner_environment
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner


@pytest_asyncio.fixture
async def fleet_database():
    original = base_fleet_database.__wrapped__()
    engine, sf, schema = await anext(original)
    try:
        yield engine, sf, schema
    finally:
        await original.aclose()
        evidence = os.environ.get("BC01_EVIDENCE_DIR")
        if evidence:
            Path(evidence, "owned-schema.json").write_text(json.dumps({"owned_schema": schema, "created": True, "dropped": True}, indent=2))


def bound_service(item):
    from deerflow_ecs_fleet.job_service import FleetJobService

    from app.fleet.job_tracking import read_tracking
    from app.fleet.mutation import FleetMutationCapability

    capability = FleetMutationCapability(item.identity, item.spec)
    kwargs = {}
    if "parent_capability" in inspect.signature(FleetJobService).parameters:
        from types import SimpleNamespace

        from app.fleet.job_tracking import bind_private_fleet_driver
        from deerflow.mcp.tasks import McpTaskDriverRegistry

        config = item.env[3].config.model_copy(update={"continuations_enabled": True})
        drivers = McpTaskDriverRegistry()
        plugin = SimpleNamespace(enabled=True, use="deerflow_ecs_fleet:install", config=config.model_dump(mode="python"))
        approved = bind_private_fleet_driver(item.env[1], drivers, capability, [plugin])
        assert approved.continuations_enabled
        return drivers.get("fleet").jobs, capability
    config = item.env[3].config.model_copy(update={"continuations_enabled": True})
    return FleetJobService(item.env[1], config, tracking_reader=read_tracking, **kwargs), capability


async def submit(item, service, capability, invocation, mode="awaited", **changes):
    from deerflow_ecs_fleet.mcp_driver import FleetTaskDriver

    from deerflow.mcp.tasks import TaskSubmitRequest
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    request = TaskSubmitRequest(
        user_id=item.spec.user_id,
        thread_id=item.spec.thread_id,
        run_id=item.spec.run_id,
        local_task_id="tracking-" + invocation,
        tool_call_id="tool-" + invocation,
        server_name="fleet",
        task_name="child",
        arguments={"task_name": "child", "profile": "batch", "argv": ["/bin/true"], "link_mode": mode},
        driver_data={"invocation_id": invocation, "agent_task_id": "forged-task", "generation": 999},
    )
    from dataclasses import replace
    from types import SimpleNamespace

    from app.mcp_tasks import McpTaskService
    from deerflow.mcp.tasks import McpTaskDriverRegistry
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository

    drivers = McpTaskDriverRegistry()
    drivers.register("fleet", FleetTaskDriver(service))
    submitter = McpTaskService(repository=McpTaskRepository(item.env[1], mutation_capability=capability), drivers=drivers, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    with remote_mutation_scope(capability.context):
        created = await submitter.submit(driver_name="fleet", request=replace(request, **changes))
    return SimpleNamespace(remote_task_id=created["remote_task_id"], tracking_task_id=created["id"])


async def count(item, table):
    async with item.engine.connect() as connection:
        exists = await connection.scalar(text("SELECT to_regclass(:name)"), {"name": table})
        return await connection.scalar(text("SELECT count(*) FROM " + table)) if exists else 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc01_contract(checkpoint_owner):
    item = checkpoint_owner
    from deerflow.persistence.mcp_tasks.model import McpTaskRow

    async with item.engine.begin() as connection:
        await connection.run_sync(lambda sync: McpTaskRow.__table__.create(sync))
    service, capability = bound_service(item)
    from types import SimpleNamespace

    from deerflow_ecs_fleet.mcp_driver import FleetTaskDriver
    from langchain_core.messages import AIMessage
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    from app.mcp_tasks import McpTaskService
    from deerflow.mcp.tasks import McpTaskDriverRegistry
    from deerflow.mcp.tasks.fleet_runtime import fleet_job_submitter_scope, get_fleet_job_submitter
    from deerflow.persistence.mcp_tasks.sql import McpTaskRepository
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.tools.builtins.fleet_jobs import submit_fleet_job

    drivers = McpTaskDriverRegistry()
    drivers.register("fleet", FleetTaskDriver(service))
    repository = McpTaskRepository(item.env[1], mutation_capability=capability)
    submitter = McpTaskService(repository=repository, drivers=drivers, poll_interval_seconds=1, lease_seconds=30, max_concurrent_polls=1)
    builder = StateGraph(MessagesState, context_schema=dict)
    builder.add_node("tools", ToolNode([submit_fleet_job], handle_tool_errors=False))
    builder.add_edge(START, "tools")
    builder.add_edge("tools", END)
    graph = builder.compile(checkpointer=InMemorySaver())
    with remote_mutation_scope(capability.context), fleet_job_submitter_scope(submitter, profile_names=("batch",)):
        assert get_fleet_job_submitter() is submitter
        result = await graph.ainvoke(
            {
                "messages": [
                    AIMessage(content="", tool_calls=[{"name": "submit_fleet_job", "args": {"task_name": "child", "profile": "batch", "argv": ["/bin/true"], "link_mode": "awaited"}, "id": "original-tool-call", "type": "tool_call"}])
                ]
            },
            {"configurable": {"thread_id": item.spec.thread_id}},
            context={"user_id": item.spec.user_id, "run_id": item.spec.run_id},
        )
    assert result["messages"][-1].tool_call_id == "original-tool-call"
    async with item.engine.connect() as connection:
        first = SimpleNamespace(remote_task_id=await connection.scalar(text("SELECT id FROM fleet_jobs")))
    second = await submit(item, service, capability, "second", "detached")
    assert await count(item, "fleet_job_links") == 2, "Submission must commit owned immutable child links alongside jobs"
    from deerflow_ecs_fleet.persistence.wait_groups import WaitGroups

    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    groups = WaitGroups(item.env[1], parent_capability=service.parent_capability)
    with remote_mutation_scope(capability.context):
        group = await groups.seal(continuation_key="continuation-one", job_ids=[first.remote_task_id])
        retry = await groups.seal(continuation_key="continuation-one", job_ids=[second.remote_task_id])
    assert group == retry
    async with item.engine.connect() as connection:
        links = (await connection.execute(text("SELECT agent_task_id,generation,parent_run_id,job_id,link_mode FROM fleet_job_links ORDER BY job_id"))).mappings().all()
        assert {row["job_id"] for row in links} == {first.remote_task_id, second.remote_task_id}
        assert all((row["agent_task_id"], row["generation"], row["parent_run_id"]) == (item.spec.agent_task_id, item.spec.generation, item.spec.run_id) for row in links)
        assert await connection.scalar(text("SELECT version_num FROM fleet_alembic_version")) == "f0013_continuation_receipt"
        stored = (await connection.execute(text("SELECT job_ids FROM fleet_wait_groups WHERE continuation_key='continuation-one'"))).scalar_one()
        assert stored == [first.remote_task_id]
        assert await connection.scalar(text("SELECT count(*) FROM fleet_wait_groups WHERE continuation_key='continuation-one'")) == 1
    evidence = os.environ.get("BC01_EVIDENCE_DIR")
    if evidence:
        Path(evidence, "observed.json").write_text(
            json.dumps(
                {
                    "owned_task_id": item.spec.agent_task_id,
                    "owned_run_id": item.spec.run_id,
                    "job_ids": [first.remote_task_id, second.remote_task_id],
                    "sealed_group_changed": group != retry,
                    "group_count_for_key": await count(item, "fleet_wait_groups"),
                },
                indent=2,
            )
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc01_ownership_and_transaction_boundaries(checkpoint_owner):
    import asyncio
    import hashlib
    from types import SimpleNamespace

    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.persistence.wait_groups import WaitGroups
    from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity
    from sqlalchemy.exc import DBAPIError

    from app.gateway import services
    from deerflow.mcp.tasks import fleet_runtime as bridge
    from deerflow.persistence.mcp_tasks.model import McpTaskRow
    from deerflow.persistence.run import RunRepository
    from deerflow.runtime import RunManager
    from deerflow.runtime.execution.mutation_context import OwnershipRejected, remote_mutation_scope
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    from .test_c02_remote_agent_admission import backend, body, request

    item = checkpoint_owner
    async with item.engine.begin() as connection:
        await connection.run_sync(lambda sync: McpTaskRow.__table__.create(sync))
        await connection.execute(text("UPDATE fleet_nodes SET cpu_millis=4000,memory_mib=8192,agent_limit=2"))
    service, capability = bound_service(item)
    # A private execution must never use the Gateway-global submission service.
    bridge.set_fleet_job_submitter(service, profile_names=("batch",))
    try:
        with remote_mutation_scope(capability.context):
            with pytest.raises(OwnershipRejected):
                bridge.get_fleet_job_submitter()
    finally:
        bridge.set_fleet_job_submitter(None)

    from deerflow_ecs_fleet.job_service import FleetJobService
    from deerflow_ecs_fleet.protocol import JobSpec

    from app.fleet.job_tracking import read_tracking

    local = FleetJobService(item.env[1], service.config, tracking_reader=read_tracking)
    local_job = await local.submit(
        user_id="local-user", thread_id="local-thread", source_run_id=None, tracking_task_id="local-tracking", idempotency_key="local-detached", spec=JobSpec(task_name="local", profile="batch", argv=["/bin/true"])
    )
    assert local_job["state"] == "staged"
    assert await count(item, "fleet_job_links") == 0
    with pytest.raises(PermissionError):
        await local.submit(
            user_id="local-user",
            thread_id="local-thread",
            source_run_id=None,
            tracking_task_id="invalid-tracking",
            idempotency_key="invalid-awaited",
            spec=JobSpec(task_name="local", profile="batch", argv=["/bin/true"], link_mode="awaited"),
        )

    first = await submit(item, service, capability, "original")
    other_user = SimpleNamespace(id="bc01-other-user", system_role="admin")
    token = set_current_user(other_user)
    try:
        app = item.env[4]
        record = await services.start_run(body(), "bc01-other-thread", request(RunManager(store=RunRepository(item.env[1])), other_user), execution_backend=backend())
    finally:
        reset_current_user(token)
    accepted = await app.state.fleet_ownership.claim_agent("node-c03", node_session_id=item.env[6], worker=item.env[8])
    await app.state.fleet_ownership.authorize_start(node_id="node-c03", node_session_id=item.env[6], attempt_id=accepted.attempt_id, token=accepted.token)
    async with item.engine.connect() as connection:
        payload = await connection.scalar(text("SELECT payload FROM fleet_launch_specs WHERE run_id=:run"), {"run": record.run_id})
    other = SimpleNamespace(
        engine=item.engine,
        env=item.env,
        spec=LaunchSpec.model_validate(payload),
        identity=ExecutionIdentity(
            node_id="node-c03",
            node_session_id=item.env[6],
            agent_task_id=accepted.agent_task_id,
            generation=accepted.launch_spec["generation"],
            attempt_id=accepted.attempt_id,
            owner_worker_id=accepted.owner_worker_id,
            token_stamp=hashlib.sha256(accepted.token.encode()).hexdigest(),
        ),
    )
    other_service, other_capability = bound_service(other)
    foreign = await submit(other, other_service, other_capability, "foreign")
    groups = WaitGroups(item.env[1], parent_capability=service.parent_capability)
    with remote_mutation_scope(capability.context):
        with pytest.raises(PermissionError) as cross_rejection:
            await groups.seal(continuation_key="cross-task", job_ids=[foreign.remote_task_id])
        sealed = await asyncio.gather(*(groups.seal(continuation_key="concurrent-key", job_ids=[first.remote_task_id]) for _ in range(2)))
    assert sealed[0] == sealed[1]
    with remote_mutation_scope(other_capability.context):
        with pytest.raises(PermissionError):
            await WaitGroups(item.env[1], parent_capability=other_service.parent_capability).seal(continuation_key="concurrent-key", job_ids=[foreign.remote_task_id])
    before = (await count(item, "fleet_jobs"), await count(item, "fleet_job_links"), await count(item, "fleet_job_invocations"), await count(item, "mcp_tasks"))
    for changes in ({"user_id": "wrong-user"}, {"thread_id": "wrong-thread"}, {"run_id": "wrong-run"}):
        with pytest.raises(OwnershipRejected):
            await submit(item, service, capability, "wrong-target", **changes)
    async with item.engine.begin() as connection:
        await connection.execute(text("UPDATE fleet_agent_tasks SET generation=generation+1 WHERE id=:id"), {"id": item.spec.agent_task_id})
    with pytest.raises(OwnershipRejected):
        await submit(item, service, capability, "stale")
    assert before == (await count(item, "fleet_jobs"), await count(item, "fleet_job_links"), await count(item, "fleet_job_invocations"), await count(item, "mcp_tasks"))
    async with item.engine.begin() as connection:
        await connection.execute(text("UPDATE fleet_agent_tasks SET generation=generation-1 WHERE id=:id"), {"id": item.spec.agent_task_id})
    for sql in ("UPDATE fleet_job_links SET link_mode='detached'", "DELETE FROM fleet_job_links", "UPDATE fleet_wait_groups SET job_ids='[]'", "DELETE FROM fleet_wait_groups"):
        with pytest.raises(DBAPIError):
            async with item.engine.begin() as connection:
                await connection.execute(text(sql))
    # Proof/state fields remain available to BC02; only sealed identity is frozen.
    async with item.engine.begin() as connection:
        await connection.execute(text("UPDATE fleet_wait_groups SET checkpoint_id='next-proof',state='waiting_jobs'"))
        await connection.execute(
            text("""CREATE FUNCTION bc01_expire_parent() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          UPDATE runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=NEW.parent_run_id;
          RETURN NEW; END $$""")
        )
        await connection.execute(text("CREATE TRIGGER bc01_expire AFTER INSERT ON fleet_job_links FOR EACH ROW EXECUTE FUNCTION bc01_expire_parent()"))
    with pytest.raises(OwnershipRejected):
        await submit(item, service, capability, "postflush-revocation")
    assert before == (await count(item, "fleet_jobs"), await count(item, "fleet_job_links"), await count(item, "fleet_job_invocations"), await count(item, "mcp_tasks"))
    async with item.engine.connect() as connection:
        assert await connection.scalar(text("SELECT lease_expires_at>clock_timestamp() FROM runs WHERE run_id=:run"), {"run": item.spec.run_id})
        assert await connection.scalar(text("SELECT count(*) FROM fleet_wait_groups WHERE continuation_key='concurrent-key'")) == 1
        assert await connection.scalar(text("SELECT count(*) FROM fleet_wait_groups WHERE continuation_key='cross-task'")) == 0

    evidence = os.environ.get("BC01_EVIDENCE_DIR")
    if evidence:
        async with item.engine.connect() as connection:
            observed = {
                "original_task_id": item.spec.agent_task_id,
                "other_task_id": other.spec.agent_task_id,
                "original_user_id": item.spec.user_id,
                "other_user_id": other.spec.user_id,
                "cross_task_rejection_type": type(cross_rejection.value).__name__,
                "cross_task_rejection": str(cross_rejection.value),
                "public_seal_http_executed": False,
                "sealed_group_changed": sealed[0] != sealed[1],
                "group_count_for_key": await connection.scalar(text("SELECT count(*) FROM fleet_wait_groups WHERE continuation_key='concurrent-key'")),
                "cross_task_group_count": await connection.scalar(text("SELECT count(*) FROM fleet_wait_groups WHERE continuation_key='cross-task'")),
                "local_detached_state": local_job["state"],
                "original_lease_valid_after_rollback": await connection.scalar(text("SELECT lease_expires_at>clock_timestamp() FROM runs WHERE run_id=:run"), {"run": item.spec.run_id}),
                "rows_before_stale_and_revocation": before,
                "rows_after_stale_and_revocation": (await count(item, "fleet_jobs"), await count(item, "fleet_job_links"), await count(item, "fleet_job_invocations"), await count(item, "mcp_tasks")),
            }
        Path(evidence, "observed.json").write_text(json.dumps(observed, indent=2))
