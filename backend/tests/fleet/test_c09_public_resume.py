"""One native public keyed resume diagnostic; installed physical proof is separate."""

import httpx
import pytest
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import owner_environment as owner_environment
from .test_c09_stale_prepared_publication import checkpoint_owner as checkpoint_owner


@pytest.mark.asyncio
async def test_stock_pause_public_keyed_resume_keeps_original_task(checkpoint_owner, tmp_path, monkeypatch):
    from app.gateway.internal_auth import create_internal_auth_headers
    from app.gateway.routers.thread_runs import router
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime.runs.manager import RunManager

    from .test_c02_remote_agent_admission import request
    from .test_c08_stock_workspace_boundary import test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final

    item = checkpoint_owner
    native = await test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final(item, tmp_path, monkeypatch, "full", ["values"], True, goal_continuation=True, return_fixture=True, recurrent_pause=True)
    app = item.env[4]
    app.include_router(router)
    manager = RunManager(store=app.state.run_store)
    req = request(manager, item.env[2])
    for name, value in vars(req.app.state).items():
        setattr(app.state, name, value)
    app.state.thread_store = ThreadMetaRepository(item.env[1])
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

    monkeypatch.setattr("app.gateway.deps._cached_repo", SQLiteUserRepository(item.env[1]))
    monkeypatch.setattr("app.gateway.deps._cached_local_provider", None)
    app.state.checkpointer = item.writer
    monkeypatch.setattr("app.gateway.services.resolve_agent_factory", lambda _: lambda **kwargs: native.graph)
    headers = create_internal_auth_headers(owner_user_id=item.spec.user_id)
    headers["X-CSRF-Token"] = "native-resume-csrf"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://native", headers=headers, cookies={"csrf_token": "native-resume-csrf"}) as client:
        resolver = app.state.bound_run_execution_backend
        original_config = resolver.config
        profile = original_config.profiles[item.spec.profile]
        resolver.config = original_config.model_copy(update={"profiles": original_config.profiles | {item.spec.profile: profile.model_copy(update={"runtime_digest": "sha256:" + "f" * 64})}})
        try:
            rejected = await client.post(f"/api/threads/{item.spec.thread_id}/runs", json={"command": {"resume": {native.snapshot.interrupts[0].id: "approved"}}, "stream_mode": ["values"]})
            assert rejected.status_code == 409, rejected.text
            assert rejected.json()["detail"] == "Bound remote execution profile changed"
            async with item.env[1]() as session:
                assert await session.scalar(text("SELECT count(*) FROM runs")) == 1
                assert await session.scalar(text("SELECT generation FROM fleet_agent_tasks")) == item.spec.generation
        finally:
            resolver.config = original_config
        from app.fleet.execution import FleetRunAdmission

        # Capture actual durable rows, not only counts: rejection must preserve
        # the original source and every admission/publication participant.
        tables = (
            "runs",
            "fleet_agent_tasks",
            "fleet_run_placements",
            "fleet_launch_specs",
            "fleet_workspace_requests",
            "fleet_workspace_points",
            "fleet_workspace_manifests",
            "fleet_attempts",
            "fleet_reservations",
            "checkpoints",
            "checkpoint_writes",
        )

        async def durable_rows():
            async with item.env[1]() as session:
                return {table: (await session.execute(text(f"SELECT row_to_json(t)::text FROM {table} t ORDER BY row_to_json(t)::text"))).scalars().all() for table in tables}

        async def public_resume(snapshot):
            return await client.post(f"/api/threads/{item.spec.thread_id}/runs", json={"command": {"resume": {snapshot.interrupts[0].id: "approved"}}, "stream_mode": ["values"]})

        baseline = await durable_rows()
        async with item.env[1]() as session:
            original = (await session.execute(text("SELECT accepted_workspace_point_id,generation FROM fleet_agent_tasks"))).one()
            original_stopped = await session.scalar(text("SELECT stopped_at FROM fleet_attempts"))
        controls = []
        # Each reachable fault is independent. Historical Node-session fences
        # carry from C08; public human resume has no caller session selector.
        for name, change, restore, values in (
            ("missing_accepted_point", "UPDATE fleet_agent_tasks SET accepted_workspace_point_id=NULL", "UPDATE fleet_agent_tasks SET accepted_workspace_point_id=:value", {"value": original.accepted_workspace_point_id}),
            ("unconfirmed_stopped_at", "UPDATE fleet_attempts SET stopped_at=NULL", "UPDATE fleet_attempts SET stopped_at=:value", {"value": original_stopped}),
            ("task_generation_mismatch", "UPDATE fleet_agent_tasks SET generation=generation+1", "UPDATE fleet_agent_tasks SET generation=:value", {"value": original.generation}),
        ):
            async with item.env[1].begin() as session:
                await session.execute(text(change))
            try:
                fault_rows = await durable_rows()
                rejected = await public_resume(native.snapshot)
                assert rejected.status_code == 409, (name, rejected.text)
                assert rejected.json()["detail"] == "Paused remote execution requires its accepted stopped source"
                assert await durable_rows() == fault_rows, name
                controls.append(name)
            finally:
                async with item.env[1].begin() as session:
                    await session.execute(text(restore), values)
            assert await durable_rows() == baseline, name

        original_insert = FleetRunAdmission.insert
        injected = RuntimeError("c09 original admission insert rollback control")
        inserts = []

        async def fail_after_original_insert(participant, session, admitted_run):
            await original_insert(participant, session, admitted_run)
            await session.flush()
            assert await session.scalar(text("SELECT count(*) FROM runs")) == 2
            assert await session.scalar(text("SELECT count(*) FROM fleet_run_placements")) == 2
            assert await session.scalar(text("SELECT count(*) FROM fleet_launch_specs")) == 2
            assert await session.scalar(text("SELECT generation FROM fleet_agent_tasks")) == item.spec.generation + 1
            inserts.append(admitted_run["run_id"])
            raise injected

        monkeypatch.setattr(FleetRunAdmission, "insert", fail_after_original_insert)
        try:
            with pytest.raises(RuntimeError) as failure:
                await public_resume(native.snapshot)
            assert failure.value is injected and len(inserts) == 1
        finally:
            monkeypatch.setattr(FleetRunAdmission, "insert", original_insert)
        assert await durable_rows() == baseline
        controls.append("original_admission_insert_transaction_rollback")

        import asyncio

        responses = await asyncio.gather(public_resume(native.snapshot), public_resume(native.snapshot))
        assert sorted(response.status_code for response in responses) == [200, 409], [response.text for response in responses]
        response = next(response for response in responses if response.status_code == 200)
        run_id = response.json()["run_id"]
        async with item.env[1]() as session:
            assert await session.scalar(text("SELECT count(*) FROM runs")) == 2
            assert await session.scalar(text("SELECT count(*) FROM fleet_run_placements")) == 2
            assert await session.scalar(text("SELECT count(*) FROM fleet_launch_specs")) == 2
        controls.append("concurrent_public_resume_single_winner")
    async with item.env[1]() as session:
        row = (
            await session.execute(
                text("SELECT t.id,t.generation,t.current_run_id,p.generation,l.payload FROM fleet_agent_tasks t JOIN fleet_run_placements p ON p.run_id=t.current_run_id JOIN fleet_launch_specs l ON l.id=p.launch_spec_ref")
            )
        ).one()
        assert row.id == item.spec.agent_task_id and row.generation == item.spec.generation + 1
        assert row.current_run_id == run_id and run_id != item.spec.run_id
        source_point_id = await session.scalar(text("SELECT id FROM fleet_workspace_points WHERE kind='paused'"))
        assert row.payload["source_workspace_point_id"] == source_point_id
        assert row.payload["source_workspace_checkpoint_id"] == native.snapshot.config["configurable"]["checkpoint_id"]
        assert await session.scalar(text("SELECT continuation_budget FROM fleet_agent_tasks")) == 0
        from datetime import datetime

        assert datetime.fromisoformat(row.payload["execution_deadline"].replace("Z", "+00:00")) == item.spec.execution_deadline
        assert native.completed == ["completed"]

    import asyncio
    import hashlib
    from types import SimpleNamespace

    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity
    from deerflow_ecs_fleet.worker.client import NodeClient

    from app.fleet.runner_context import FleetCheckpointFence
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer

    from .c09_integration_fixture import original_native_execution
    from .test_c03_remote_agent_admission import claim
    from .test_c08_final_stop_authority import durable_rows
    from .test_c09_stale_prepared_publication import retain_publication_cleanup

    # Original generation is physically stopped before a new session claims
    # the already-admitted resume. Never rotate a running generation's session.
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://node/") as node_http:
        restarted_node = NodeClient(gateway_url="http://node", credential=item.env[7].token, http_client=node_http)
        await restarted_node.open_session()
        current_session = restarted_node.session_id
        assert (await restarted_node.heartbeat())["health"] == "online"
    assert current_session != item.identity.node_session_id
    claim_env = item.env[:6] + (current_session,) + item.env[7:]
    owners = []
    snapshot = native.snapshot
    source_point = source_point_id
    graph = native.resume_graph
    for generation in (item.spec.generation + 1, item.spec.generation + 2):
        accepted = await claim(claim_env)
        assert accepted is not None and accepted.run_id == run_id
        grant = await app.state.fleet_ownership.authorize_start(node_id=item.identity.node_id, node_session_id=current_session, attempt_id=accepted.attempt_id, token=accepted.token)
        spec = LaunchSpec.model_validate(accepted.launch_spec)
        assert spec.agent_task_id == item.spec.agent_task_id and spec.generation == generation
        assert spec.source_workspace_point_id == source_point
        assert spec.source_workspace_checkpoint_id == snapshot.config["configurable"]["checkpoint_id"]
        assert spec.execution_deadline == item.spec.execution_deadline
        identity = ExecutionIdentity(
            node_id=item.identity.node_id,
            node_session_id=current_session,
            agent_task_id=spec.agent_task_id,
            generation=spec.generation,
            attempt_id=accepted.attempt_id,
            owner_worker_id=accepted.owner_worker_id,
            token_stamp=hashlib.sha256(accepted.token.encode()).hexdigest(),
        )
        if generation == item.spec.generation + 1:
            before_late_ack = await durable_rows(SimpleNamespace(sf=item.env[1]))
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://node/") as node_http:
                node = NodeClient(gateway_url="http://node", credential=item.env[7].token, http_client=node_http)
                node.node_id, node.session_id = item.identity.node_id, current_session
                old_claim = {"kind": "agent", "attempt_id": item.accepted.attempt_id, "token": item.accepted.token}
                stop_fields = {"reason": "lease_lost", "exit_code": 137, "process_ref": "fleet-" + item.accepted.attempt_id, "physical_stopped": True}
                with pytest.raises(httpx.HTTPStatusError) as ordinary_denied:
                    await node.attempt(old_claim, "stopped", **stop_fields)
                assert ordinary_denied.value.response.status_code == 409
                receipt = await node.reconcile_stopped(old_claim, original_node_session_id=item.identity.node_session_id, **stop_fields)
                assert receipt == {"state": "cancelled", "stopped": True}
            assert await durable_rows(SimpleNamespace(sf=item.env[1])) == before_late_ack
            async with item.env[1]() as session:
                assert await session.scalar(text("SELECT state FROM fleet_reservations WHERE attempt_id=:id"), {"id": accepted.attempt_id}) in {"reserved", "active"}
        writer_owner = make_checkpointer(item.private, write_fence=FleetCheckpointFence(identity, spec))
        writer = await writer_owner.__aenter__()
        try:
            resumed = SimpleNamespace(engine=item.engine, env=claim_env, private=item.private, writer=writer, identity=identity, spec=spec, grant=grant, accepted=accepted)
            execution = await original_native_execution(resumed, tmp_path, monkeypatch, graph_override=graph, wait_entered=False)
        except BaseException:
            import sys

            from .c08_installed_cleanup import settle_owned_cleanup

            await settle_owned_cleanup([("untransferred-resume-writer", lambda: writer_owner.__aexit__(None, None, None))], original_error=sys.exception())
            raise
        # Transfer synchronously before any await. Outer checkpoint_owner keeps
        # the SAME PG scopes until the current actual runner/Node/stack settles.
        execution.teardown.stack.push_async_exit(writer_owner)
        item.publication_cleanup["resume_writer_owners"] = owners
        owners.append((execution, writer_owner))
        try:
            completed = await asyncio.wait_for(asyncio.shield(execution.execute), 20)
            repaused = generation == item.spec.generation + 1
            assert completed.status.value == ("interrupted" if repaused else "success"), completed.error
            snapshot = await execution.publisher.accessor.aget({"configurable": {"thread_id": spec.thread_id}})
            assert bool(snapshot.interrupts) == repaused and bool(snapshot.next) == repaused
            assert native.completed == ["completed"]
            assert native.questions == (["entered", "entered", "entered"] if repaused else ["entered", "entered", "entered", "entered"])
            from langchain_core.messages import ToolMessage

            assert sum(isinstance(message, ToolMessage) and message.content == "approved" for message in snapshot.values["messages"]) == (1 if repaused else 2)
            execution.stop_node.set()
            await execution.node
            stopped = await app.state.fleet_ownership.stopped(
                reason="exit", exit_code=0, process_ref="fleet-" + accepted.attempt_id, physical_stopped=True, node_id=identity.node_id, node_session_id=identity.node_session_id, attempt_id=identity.attempt_id, token=accepted.token
            )
            assert stopped["stopped"]
            async with item.env[1]() as session:
                assert await session.scalar(text("SELECT state FROM fleet_agent_tasks")) == ("input_required" if repaused else "succeeded")
                assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE kind='paused'")) == 2
                assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points WHERE kind='final'")) == (0 if repaused else 1)
                assert await session.scalar(text("SELECT count(*) FROM fleet_reservations WHERE state != 'released'")) == 0
                source_point = await session.scalar(text("SELECT accepted_workspace_point_id FROM fleet_agent_tasks"))
                assert await session.scalar(text("SELECT continuation_budget FROM fleet_agent_tasks")) == 0
        finally:
            import sys

            primary = sys.exception()
            try:
                await retain_publication_cleanup(execution, None, None, None, execution.release.set, item.publication_cleanup, original_error=primary)
            except BaseException as cleanup_error:
                if primary is not None and cleanup_error is not primary:
                    raise BaseExceptionGroup("Original resume and retained cleanup failures", [primary, cleanup_error])
                raise
        assert execution.execute.done() and execution.node.done() and execution.teardown.closed and not execution.teardown._phases
        if repaused:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://native", headers=headers, cookies={"csrf_token": "native-resume-csrf"}) as client:
                response = await public_resume(snapshot)
                assert response.status_code == 200, response.text
                run_id = response.json()["run_id"]
            graph = native.final_resume_graph
    import json

    print(
        json.dumps(
            {
                "scope": "native original public admission controls and recurrent stock pause; no installed Docker claim",
                "controls": controls,
                "task_id": item.spec.agent_task_id,
                "generation": spec.generation,
                "runs": 3,
                "cached_sideeffects": len(native.completed),
                "recurrent_pause": True,
                "owners_done": all(owner.execute.done() and owner.node.done() and owner.teardown.closed and not owner.teardown._phases for owner, _ in owners),
            }
        )
    )
