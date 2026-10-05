"""Actual healthy C branch restores accepted bytes into an owned child."""

import httpx
import pytest
from fastapi import FastAPI
from langchain_core.messages import AIMessage, HumanMessage
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment
from .test_c08_terminal_pair import participant
from .test_c08_terminal_pair import prepared_pair as prepared_pair


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prepared_pair", [dict(messages=[HumanMessage(content="question", id="human"), AIMessage(content="answer", id="answer")], files={"outputs/result": b"accepted", "uploads/input": b"upload", "workspace/work": b"work"})], indirect=True
)
@pytest.mark.parametrize(
    "mutation",
    [
        "none",
        "digest",
        "thread-meta-failure",
        "legacy-missing-origin",
        "ancestor-origin-missing",
        "ancestor-origin-conflict",
        "ancestor-target-conflict",
        "ancestor-parent-map",
        "post-copy-cancel",
        "ancestor-private-source",
        "sibling-cancel",
        "sibling-sql-failure",
        "sibling-repeat-cancel",
    ],
)
async def test_actual_terminal_fleet_branch_clones_source_and_owns_routing(prepared_pair, tmp_path, monkeypatch, mutation):
    from app.fleet.ownership import install_fleet_ownership
    from app.gateway.routers import artifacts, fleet_artifacts, threads
    from deerflow.config.paths import Paths
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime import RunManager
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer

    from .test_c02_remote_agent_admission import request

    p = prepared_pair
    with p.scope():
        await RunRepository(p.sf, mutation_capability=p.capability, terminal_participant=participant(p)).update_status(p.identity.run_id, "success")
    env = p.item.env
    await env[4].state.fleet_ownership.stopped(
        reason="exit", exit_code=0, process_ref=p.identity.process_ref, physical_stopped=True, node_id=p.identity.node_id, node_session_id=p.identity.node_session_id, attempt_id=p.identity.attempt_id, token=p.item.accepted.token
    )
    paths = Paths(tmp_path / "local-host")
    monkeypatch.setattr(threads, "get_paths", lambda: paths)
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: paths)
    local_neighbor = paths.sandbox_outputs_dir(p.identity.thread_id, user_id=p.identity.user_id)
    local_neighbor.mkdir(parents=True)
    (local_neighbor / "result").write_bytes(b"local neighbor")
    b_neighbor = p.versions.nas.root / "b-neighbor"
    b_neighbor.write_bytes(b"B neighbor")
    manager = RunManager(store=env[4].state.run_store)
    req = request(manager, env[2])
    app = FastAPI()
    for key, value in vars(req.app.state).items():
        setattr(app.state, key, value)
    app.state.extensions = env[4].state.extensions
    app.state.extensions.services[0][1].config = app.state.extensions.services[0][1].config.model_copy(update={"nas_root": p.versions.nas.root, "nas_identity": "task4"})
    app.state.run_store = env[4].state.run_store
    install_fleet_ownership(app, p.sf)
    if mutation == "legacy-missing-origin":
        from deerflow.persistence.thread_meta.model import ThreadMetaRow
        from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

        async with p.item.engine.begin() as connection:
            await connection.run_sync(lambda sync: ThreadMetaRow.__table__.create(sync, checkfirst=True))
        app.state.thread_store = ThreadMetaRepository(p.sf)
        req.app.state.thread_store = app.state.thread_store
    await app.state.thread_store.create(p.identity.thread_id, user_id=p.identity.user_id)

    @app.middleware("http")
    async def authenticated_fixture(request, call_next):
        request.state.user = env[2]
        request.state.auth_source = "session"
        return await call_next(request)

    app.include_router(threads.router)
    app.include_router(artifacts.router)
    app.include_router(fleet_artifacts.router)
    async with make_checkpointer(p.item.private) as saver:
        app.state.checkpointer = saver
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            if mutation == "thread-meta-failure":
                original_create = app.state.thread_store.create

                async def rejected_child_metadata(new_id, *args, **kwargs):
                    if new_id != p.identity.thread_id:
                        raise OSError("isolated child metadata failure")
                    return await original_create(new_id, *args, **kwargs)

                monkeypatch.setattr(app.state.thread_store, "create", rejected_child_metadata)
            if mutation == "digest":
                (p.versions.nas.root / p.candidate.nas_prefix / "outputs/result").write_bytes(b"ACCEPTED")
            if mutation in {"sibling-cancel", "sibling-sql-failure", "sibling-repeat-cancel"}:
                import asyncio

                from sqlalchemy.exc import DBAPIError

                entered = asyncio.Event()
                cloned = []
                original_restore = app.state.fleet_workspace_files.restore_branch

                async def observed_restore(manifest, target):
                    await original_restore(manifest, target)
                    cloned.append(target)
                    assert (target / "outputs/result").read_bytes() == b"accepted"

                async def broken_siblings(store, parent):
                    assert parent == p.identity.thread_id and len(cloned) == 1
                    entered.set()
                    if mutation in {"sibling-cancel", "sibling-repeat-cancel"}:
                        await asyncio.Event().wait()
                    # An actual isolated PostgreSQL query failure propagates
                    # through the original HTTP branch request.
                    async with p.sf() as session:
                        await session.execute(text("SELECT * FROM c08_deliberately_absent_sibling_table"))

                monkeypatch.setattr(app.state.fleet_workspace_files, "restore_branch", observed_restore)
                monkeypatch.setattr(threads, "_branch_sibling_records", broken_siblings)
                import shutil
                import threading

                cleanup_entered, cleanup_release, cleanup_done = threading.Event(), threading.Event(), threading.Event()
                original_rmtree = shutil.rmtree

                def held_cleanup(target, *args, **kwargs):
                    assert target == cloned[0]
                    cleanup_entered.set()
                    assert cleanup_release.wait(5)
                    try:
                        return original_rmtree(target, *args, **kwargs)
                    finally:
                        cleanup_done.set()

                if mutation == "sibling-repeat-cancel":
                    monkeypatch.setattr(shutil, "rmtree", held_cleanup)
                pending = asyncio.create_task(client.post(f"/api/threads/{p.identity.thread_id}/branches", json={"message_id": "answer"}))
                try:
                    await asyncio.wait_for(entered.wait(), 5)
                    if mutation in {"sibling-cancel", "sibling-repeat-cancel"}:
                        pending.cancel()
                        if mutation == "sibling-repeat-cancel":
                            assert await asyncio.to_thread(cleanup_entered.wait, 5)
                            assert cloned[0].is_dir()
                            pending.cancel()
                            turn = asyncio.Event()
                            asyncio.get_running_loop().call_soon(turn.set)
                            await turn.wait()
                            assert not pending.done(), "Untitled HTTP caller returned before owned cleanup settled"
                            cleanup_release.set()
                        with pytest.raises(asyncio.CancelledError):
                            await pending
                    else:
                        with pytest.raises(DBAPIError):
                            await pending
                    if mutation == "sibling-repeat-cancel":
                        assert cleanup_done.is_set()
                    assert not cloned[0].exists(), "Untitled sibling lookup failure leaked the owned clone"
                    async with p.sf() as session:
                        assert await session.scalar(text("SELECT count(*) FROM thread_execution_bindings WHERE parent_thread_id=:parent AND recovery_required"), {"parent": p.identity.thread_id}) == 1
                        assert await session.scalar(text("SELECT count(*) FROM runs WHERE thread_id=:parent AND operation_kind='branch'"), {"parent": p.identity.thread_id}) == 0
                    next_read = await client.get(f"/api/threads/{p.identity.thread_id}/fleet/manifests/{p.candidate.manifest_id}")
                    assert next_read.status_code == 200, next_read.text
                    assert (local_neighbor / "result").read_bytes() == b"local neighbor" and b_neighbor.read_bytes() == b"B neighbor"
                finally:
                    cleanup_release.set()
                    await asyncio.gather(pending, return_exceptions=True)
                return
            if mutation == "post-copy-cancel":
                import asyncio
                import shutil
                import threading

                metadata_entered = asyncio.Event()
                cleanup_entered, cleanup_release, cleanup_done = threading.Event(), threading.Event(), threading.Event()
                original_create = app.state.thread_store.create
                original_rmtree = shutil.rmtree
                cleaning = []
                releases = []
                original_delete = app.state.run_store.delete_thread_operation

                async def observed_delete(run_id, *, user_id):
                    releases.append(("entered", run_id, user_id))
                    try:
                        await original_delete(run_id, user_id=user_id)
                    except BaseException as error:
                        releases.append(("failed", type(error).__name__))
                        raise
                    else:
                        releases.append(("settled", run_id, user_id))

                monkeypatch.setattr(app.state.run_store, "delete_thread_operation", observed_delete)

                async def held_child_metadata(new_id, *args, **kwargs):
                    if new_id != p.identity.thread_id:
                        metadata_entered.set()
                        await asyncio.Event().wait()
                    return await original_create(new_id, *args, **kwargs)

                def held_cleanup(path, *args, **kwargs):
                    cleaning.append(path)
                    cleanup_entered.set()
                    assert cleanup_release.wait(5)
                    try:
                        return original_rmtree(path, *args, **kwargs)
                    finally:
                        cleanup_done.set()

                monkeypatch.setattr(app.state.thread_store, "create", held_child_metadata)
                monkeypatch.setattr(shutil, "rmtree", held_cleanup)
                pending = asyncio.create_task(client.post(f"/api/threads/{p.identity.thread_id}/branches", json={"message_id": "answer"}))
                try:
                    await asyncio.wait_for(metadata_entered.wait(), 5)
                    pending.cancel()
                    assert await asyncio.to_thread(cleanup_entered.wait, 5)
                    assert cleaning and cleaning[0].is_dir(), "Actual cloned child must exist while cleanup is held"
                    pending.cancel()
                    # Observe cancellation delivery without sleeps or a wait
                    # that lets detached cleanup compensate after caller exit.
                    turn = asyncio.Event()
                    asyncio.get_running_loop().call_soon(turn.set)
                    await turn.wait()
                    assert not pending.done(), "HTTP branch returned before owned cleanup settled"
                    cleanup_release.set()
                    with pytest.raises(asyncio.CancelledError):
                        await pending
                    assert cleanup_done.is_set() and not cleaning[0].exists()
                    import asyncpg

                    observer = await asyncpg.connect(p.item.engine.url.set(drivername="postgresql").render_as_string(hide_password=False), server_settings={"search_path": p.item.private.database.postgres_schema})
                    try:
                        assert await observer.fetchval("SELECT count(*) FROM thread_execution_bindings WHERE parent_thread_id=$1 AND recovery_required", p.identity.thread_id) == 1
                    finally:
                        await observer.close()
                    assert releases and releases[-1][0] == "settled", releases
                    # Independent SQL observation proves durable recovery, but
                    # the original app pool must also serve its next real call.
                    next_read = await client.get(f"/api/threads/{p.identity.thread_id}/fleet/manifests/{p.candidate.manifest_id}")
                    assert next_read.status_code == 200, next_read.text
                    async with p.sf() as session:
                        assert await session.scalar(text("SELECT count(*) FROM runs WHERE thread_id=:parent AND operation_kind='branch'"), {"parent": p.identity.thread_id}) == 0
                    assert (local_neighbor / "result").read_bytes() == b"local neighbor" and b_neighbor.read_bytes() == b"B neighbor"
                finally:
                    cleanup_release.set()
                    await asyncio.gather(pending, return_exceptions=True)
                    await asyncio.to_thread(cleanup_done.wait, 5)
                return
            response = await client.post(f"/api/threads/{p.identity.thread_id}/branches", json={"message_id": "answer"})
            if mutation in {"digest", "thread-meta-failure"}:
                assert response.status_code == (500 if mutation == "thread-meta-failure" else 409), response.text
                async with p.sf() as session:
                    failed = (await session.execute(text("SELECT thread_id,recovery_required FROM thread_execution_bindings WHERE parent_thread_id=:parent"), {"parent": p.identity.thread_id})).one()
                    assert failed.recovery_required is True
                    assert await session.scalar(text("SELECT count(*) FROM fleet_attempts a JOIN fleet_run_placements p ON a.run_id=p.run_id WHERE p.thread_id=:child"), {"child": failed.thread_id}) == 0
                assert not paths.sandbox_user_data_dir(failed.thread_id, user_id=p.identity.user_id).exists()
                assert (local_neighbor / "result").read_bytes() == b"local neighbor" and b_neighbor.read_bytes() == b"B neighbor"
                return
            assert response.status_code == 200, response.text
            child = response.json()["thread_id"]
            assert response.json()["workspace_clone_mode"] == "accepted_remote_workspace"
            child_data = paths.sandbox_user_data_dir(child, user_id=p.identity.user_id)
            for name, data in {"outputs/result": b"accepted", "uploads/input": b"upload", "workspace/work": b"work"}.items():
                assert (child_data / name).read_bytes() == data
                sealed = p.versions.nas.root / p.candidate.nas_prefix / name
                assert (child_data / name).stat().st_ino != sealed.stat().st_ino
                assert (child_data / name).stat().st_nlink == 1
            assert (local_neighbor / "result").read_bytes() == b"local neighbor" and b_neighbor.read_bytes() == b"B neighbor"
            async with p.sf() as session:
                binding = (await session.execute(text("SELECT backend,parent_thread_id,source_workspace FROM thread_execution_bindings WHERE thread_id=:child"), {"child": child})).one()
            assert binding.backend == "fleet" and binding.parent_thread_id == p.identity.thread_id
            assert binding.source_workspace["point_id"] == p.identity.request_id
            assert binding.source_workspace["source_checkpoint_id"] == p.identity.checkpoint_id
            service = app.state.fleet_workspace_files
            metadata, _ = await service.selected(user_id=p.identity.user_id, thread_id=child)
            assert metadata["source_thread_id"] == p.identity.thread_id
            for explicit in (None, p.identity.request_id):
                with pytest.raises(LookupError):
                    await service.selected(user_id=p.identity.user_id, thread_id=child, point_id=explicit, checkpoint_id="unrelated-checkpoint")
            artifact = await client.get(f"/api/threads/{child}/artifacts/mnt/user-data/outputs/result")
            assert artifact.content == b"accepted" and artifact.status_code == 200
            origin_manifest_url = f"/api/threads/{child}/fleet/manifests/{p.candidate.manifest_id}"
            origin_metadata = await client.get(origin_manifest_url)
            assert origin_metadata.status_code == 200, origin_metadata.text
            assert origin_metadata.json()["point_id"] == p.identity.request_id
            assert origin_metadata.json()["source_thread_id"] == p.identity.thread_id

            if mutation == "legacy-missing-origin":
                # Reproduce an actual pre-binding server branch: its real SQL
                # metadata/checkpoints/parent history remain, routing row absent.
                async with p.sf.begin() as session:
                    await session.execute(text("DELETE FROM thread_execution_bindings WHERE thread_id=:child"), {"child": child})
                from fastapi import HTTPException

                from app.gateway import services

                from .test_c02_remote_agent_admission import backend, body

                req.app.state.checkpointer = saver
                with pytest.raises(HTTPException) as conflict:
                    await services.start_run(body("legacy child distinct user turn"), child, req, execution_backend=backend())
                assert conflict.value.status_code == 409
                async with p.sf() as session:
                    assert await session.scalar(text("SELECT count(*) FROM fleet_run_placements WHERE thread_id=:child"), {"child": child}) == 0
                return
            grand_response = await client.post(f"/api/threads/{child}/branches", json={"message_id": "answer"})
            assert grand_response.status_code == 200, grand_response.text
            grandchild = grand_response.json()["thread_id"]
            grand_data = paths.sandbox_user_data_dir(grandchild, user_id=p.identity.user_id)
            for name in ("outputs/result", "uploads/input", "workspace/work"):
                assert (grand_data / name).read_bytes() == (child_data / name).read_bytes()
                assert len({(leaf.stat().st_dev, leaf.stat().st_ino) for leaf in (grand_data / name, child_data / name, p.versions.nas.root / p.candidate.nas_prefix / name)}) == 3
            assert (await client.get(f"/api/threads/{grandchild}/artifacts/mnt/user-data/outputs/result")).content == b"accepted"
            if mutation in {"ancestor-origin-missing", "ancestor-origin-conflict", "ancestor-target-conflict", "ancestor-parent-map"}:
                async with p.sf.begin() as session:
                    if mutation == "ancestor-origin-missing":
                        await session.execute(text("UPDATE thread_execution_bindings SET source_workspace=NULL WHERE thread_id=:child"), {"child": child})
                    elif mutation == "ancestor-origin-conflict":
                        await session.execute(text("UPDATE thread_execution_bindings SET source_workspace=jsonb_set(source_workspace::jsonb,'{source_checkpoint_id}','\"unrelated\"') WHERE thread_id=:child"), {"child": child})
                    elif mutation == "ancestor-target-conflict":
                        await session.execute(
                            text("UPDATE checkpoints SET metadata=jsonb_set(metadata::jsonb,'{branch_parent_thread_id}','\"unrelated\"') WHERE thread_id=:child AND checkpoint_id=:checkpoint"),
                            {"child": child, "checkpoint": binding.source_workspace["target_checkpoint_id"]},
                        )
                    else:
                        await session.execute(text("UPDATE thread_execution_bindings SET source_workspace=jsonb_set(source_workspace::jsonb,'{parent_checkpoint_id}','\"unrelated\"') WHERE thread_id=:child"), {"child": child})
                        await session.execute(
                            text("UPDATE checkpoints SET metadata=jsonb_set(metadata::jsonb,'{branch_parent_checkpoint_id}','\"unrelated\"') WHERE thread_id=:child AND checkpoint_id=:checkpoint"),
                            {"child": child, "checkpoint": binding.source_workspace["target_checkpoint_id"]},
                        )
                clones = []
                original_restore = service.restore_branch

                async def observed_restore(*args, **kwargs):
                    clones.append(args)
                    return await original_restore(*args, **kwargs)

                monkeypatch.setattr(service, "restore_branch", observed_restore)
                rejected = await client.post(f"/api/threads/{grandchild}/branches", json={"message_id": "answer"})
                assert rejected.status_code == 409, rejected.text
                assert not clones, "Invalid intermediate ancestry must reject before any clone"
                async with p.sf() as session:
                    assert await session.scalar(text("SELECT count(*) FROM thread_execution_bindings WHERE parent_thread_id=:grandchild"), {"grandchild": grandchild}) == 0
                return
            great_response = await client.post(f"/api/threads/{grandchild}/branches", json={"message_id": "answer"})
            assert great_response.status_code == 200, great_response.text
            great = great_response.json()["thread_id"]
            great_data = paths.sandbox_user_data_dir(great, user_id=p.identity.user_id)
            assert len({leaf.stat().st_ino for leaf in (great_data / "outputs/result", grand_data / "outputs/result", child_data / "outputs/result", p.versions.nas.root / p.candidate.nas_prefix / "outputs/result")}) == 4
            assert (await client.get(f"/api/threads/{great}/artifacts/mnt/user-data/outputs/result")).content == b"accepted"

            # The actual new child admission freezes the original parent point;
            # the original node HTTP grant drives the original container clone.
            from deerflow_ecs_fleet.worker.agent_containers import AgentContainers

            from app.gateway import services

            from .test_c02_remote_agent_admission import backend, body
            from .test_c03_remote_agent_admission import claim

            req.app.state.checkpointer = saver
            result = await services.start_run(body("new child user turn"), child, req, execution_backend=backend())
            async with p.sf() as session:
                payload = await session.scalar(text("SELECT payload FROM fleet_launch_specs WHERE run_id=:id"), {"id": result.run_id})
            assert payload["source_workspace_point_id"] == p.identity.request_id
            assert payload["source_workspace_thread_id"] == p.identity.thread_id
            assert payload["source_workspace_checkpoint_id"] == p.identity.checkpoint_id
            accepted = await claim((*env[:5], result, *env[6:]))
            ownership = env[4].state.fleet_ownership
            ownership.config = ownership.config.model_copy(update={"nas_root": p.versions.nas.root, "nas_identity": "task4"})
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=env[4]), base_url="http://test") as node:
                grant_response = await node.post(
                    "/api/fleet/node/attempts/" + accepted.attempt_id + "/start", headers={"Authorization": "Bearer " + env[7].token}, json={"node_session_id": p.identity.node_session_id, "token": accepted.token}
                )
            assert grant_response.status_code == 200, grant_response.text
            driver = AgentContainers(state_dir=tmp_path / "node-control", operator_config={})
            target = await driver.prepare_workspace(p.versions.nas.root, {"attempt_id": accepted.attempt_id}, grant_response.json())
            target_data = target / ".deer-flow/users" / p.identity.user_id / "threads" / child / "user-data"
            assert (target_data / "outputs/result").read_bytes() == b"accepted"
            assert (target_data / "outputs/result").stat().st_ino != (child_data / "outputs/result").stat().st_ino
            await driver.prepare_workspace(p.versions.nas.root, {"attempt_id": accepted.attempt_id}, grant_response.json())

            from fastapi import HTTPException

            with pytest.raises(HTTPException) as active_conflict:
                await services.start_run(body("blocked while original child is active"), child, req, execution_backend=backend())
            assert active_conflict.value.status_code == 409
            own = await accept_owned_child(p, accepted, grant_response.json(), target_data, req)
            own_preview = await client.get(f"/api/threads/{child}/artifacts/mnt/user-data/outputs/result")
            assert own_preview.content == b"child publication"
            origin_preview = await client.get(f"/api/threads/{child}/artifacts/mnt/user-data/outputs/result?workspace_point_id={p.identity.request_id}")
            assert origin_preview.content == b"accepted"
            published_origin = await client.get(origin_manifest_url)
            assert published_origin.status_code == 200, published_origin.text
            assert published_origin.json() == origin_metadata.json(), "Explicit original manifest URL must remain immutable after child publication"
            for conflicting in ({"manifest_id": p.candidate.manifest_id, "checkpoint_id": "unrelated"}, {"manifest_id": p.candidate.manifest_id, "point_id": own.request_id}):
                with pytest.raises(LookupError):
                    await service.selected(user_id=p.identity.user_id, thread_id=child, **conflicting)

            own_metadata, _ = await service.selected(user_id=p.identity.user_id, thread_id=child, checkpoint_id=own.checkpoint_id)
            assert (await service.selected(user_id=p.identity.user_id, thread_id=child, point_id=own.request_id, checkpoint_id=own.checkpoint_id, manifest_id=own_metadata["manifest_id"]))[0] == own_metadata
            for conflicting in ({"point_id": own.request_id, "checkpoint_id": "unrelated"}, {"point_id": own.request_id, "manifest_id": "unrelated"}, {"checkpoint_id": own.checkpoint_id, "manifest_id": "unrelated"}):
                with pytest.raises(LookupError):
                    await service.selected(user_id=p.identity.user_id, thread_id=child, **conflicting)
            if mutation == "ancestor-private-source":
                assert own_metadata["source_thread_id"] == child and own_metadata["point_id"] == own.request_id
                async with p.sf.begin() as session:
                    await session.execute(
                        text("UPDATE checkpoints SET metadata=jsonb_set(metadata::jsonb,'{deerflow_execution_run_id}','\"wrong-origin-run\"') WHERE thread_id=:thread AND checkpoint_id=:checkpoint"),
                        {"thread": p.identity.thread_id, "checkpoint": p.identity.checkpoint_id},
                    )
                # Final child-own source stays valid; only its older ancestry
                # source is corrupt. No immutable point/manifest edits occur.
                assert (await service.selected(user_id=p.identity.user_id, thread_id=child, checkpoint_id=own.checkpoint_id))[0] == own_metadata
                clones = []
                original_restore = service.restore_branch

                async def observed_restore(*args, **kwargs):
                    clones.append(args)
                    return await original_restore(*args, **kwargs)

                checked_points = []
                original_accepted_manifest = service._accepted_manifest

                async def observed_accepted_manifest(session, point):
                    checked_points.append(point.id)
                    return await original_accepted_manifest(session, point)

                monkeypatch.setattr(service, "_accepted_manifest", observed_accepted_manifest)
                monkeypatch.setattr(service, "restore_branch", observed_restore)
                rejected = await client.post(f"/api/threads/{child}/branches", json={"message_id": "answer"})
                assert rejected.status_code == 409, rejected.text
                assert checked_points == [own.request_id, p.identity.request_id]
                assert not clones
                return
            second = await services.start_run(body("second child user turn"), child, req, execution_backend=backend())
            async with p.sf() as session:
                second_payload = await session.scalar(text("SELECT payload FROM fleet_launch_specs WHERE run_id=:id"), {"id": second.run_id})
            assert second_payload["source_workspace_point_id"] == own.request_id
            assert second_payload.get("source_workspace_thread_id") is None


async def accept_owned_child(pair, accepted, grant, data, req):
    """Publish via the same original fenced saver/request/terminal contracts."""
    import hashlib
    import os
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace

    from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity
    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.persistence.workspace_points import WorkspaceRequests
    from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity
    from langgraph.checkpoint.base import empty_checkpoint

    from app.fleet.mutation import FleetCheckpointFence, FleetMutationCapability
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController

    spec = LaunchSpec.model_validate(grant["launch_spec"])
    execution = ExecutionIdentity(
        node_id=grant["node_id"],
        node_session_id=grant["node_session_id"],
        agent_task_id=grant["agent_task_id"],
        generation=grant["generation"],
        attempt_id=accepted.attempt_id,
        owner_worker_id=grant["owner_worker_id"],
        token_stamp=hashlib.sha256(accepted.token.encode()).hexdigest(),
    )
    capability = FleetMutationCapability(execution, spec)
    config = {"configurable": {"thread_id": spec.thread_id, "checkpoint_ns": ""}}
    async with make_checkpointer(pair.item.private, write_fence=FleetCheckpointFence(execution, spec)) as saver:
        previous = await saver.aget_tuple(config)
        checkpoint = empty_checkpoint() | {"channel_values": previous.checkpoint["channel_values"], "channel_versions": previous.checkpoint["channel_versions"]}
        with remote_mutation_scope(capability.context):
            config = await saver.aput(previous.config, checkpoint, {"source": "loop", "step": 2, "parents": {}}, {})
    identity = WorkspaceBoundaryIdentity.from_context(
        capability.context,
        request_id="child-final",
        checkpoint_id=config["configurable"]["checkpoint_id"],
        kind="final",
        publication_key="child-success",
        presented_paths=(),
        source_workspace_version=grant["accepted_workspace"]["manifest"]["manifest_id"],
        desired_core_status="success",
        desired_task_status="succeeded",
        desired_placement_status="succeeded",
    )
    (data / "outputs/result").write_bytes(b"child publication")
    fd = os.open(data, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        candidate = pair.versions.seal(identity, fd)
    finally:
        os.close(fd)
    requests = WorkspaceRequests()
    async with pair.sf.begin() as session:
        await requests.create(session, identity, barrier_epoch=1)
        await requests.claim(session, identity, nonce="f" * 64, barrier_epoch=1, deadline=datetime.now(UTC) + timedelta(seconds=30))
        await requests.prepared(session, identity, nonce="f" * 64, barrier_epoch=1, manifest=candidate)
    controller = WorkspaceWriterController()
    controller.execution_deadline = __import__("time").monotonic() + 30
    await controller.close_and_wait(deadline=controller.execution_deadline, final=True)
    owned = SimpleNamespace(capability=capability, identity=identity, candidate=candidate, controller=controller)
    with remote_mutation_scope(capability.context):
        await RunRepository(pair.sf, mutation_capability=capability, terminal_participant=participant(owned)).update_status(spec.run_id, "success")
    from fastapi import HTTPException

    from app.gateway import services

    from .test_c02_remote_agent_admission import backend, body

    with pytest.raises(HTTPException) as finishing_conflict:
        await services.start_run(body("blocked while child is finishing"), spec.thread_id, req, execution_backend=backend())
    assert finishing_conflict.value.status_code == 409
    await pair.item.env[4].state.fleet_ownership.stopped(
        reason="exit", exit_code=0, process_ref=grant["process_ref"], physical_stopped=True, node_id=grant["node_id"], node_session_id=grant["node_session_id"], attempt_id=accepted.attempt_id, token=accepted.token
    )
    return identity
