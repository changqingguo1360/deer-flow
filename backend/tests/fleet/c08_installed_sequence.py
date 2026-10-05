"""Actual sequential original Runner launches after an accepted stock turn."""

import asyncio
import json
import time

import httpx
from sqlalchemy import text


async def run_sequence(*, kind, app, core, user, db, native_private, backend, driver, daemon, stager, nas, original, points, evidence, tmp_path, mode):
    from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication

    from app.gateway import services
    from app.gateway.csrf_middleware import CSRF_COOKIE_NAME, CSRF_HEADER_NAME, generate_csrf_token
    from app.gateway.internal_auth import create_internal_auth_headers
    from app.gateway.routers import artifacts, fleet_artifacts, threads
    from app.gateway.routers.thread_runs import RunCreateRequest
    from deerflow.config.paths import Paths
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer

    from .test_c02_remote_agent_admission import request

    req = request(core, user)
    for key, value in vars(req.app.state).items():
        if not hasattr(app.state, key):
            setattr(app.state, key, value)
    req.app = app
    app.state.checkpoint_channel_mode = mode
    existing_thread = await app.state.thread_store.get(original.thread_id, user_id=user.id)
    assert existing_thread is not None
    assert existing_thread["thread_id"] == original.thread_id and existing_thread["user_id"] == user.id
    for router in (threads.router, artifacts.router, fleet_artifacts.router):
        app.include_router(router)
    # The branch clone is owned by this fixture's host tree. Preserve the
    # original getter and restore even when the actual child launch fails.
    paths = Paths(tmp_path / "branch-host")
    original_paths = threads.get_paths
    run_task = None
    record = None
    child_refs = []
    source = points[-1]
    async with db.engine.connect() as conn:
        row = (await conn.execute(text("SELECT nas_prefix FROM fleet_workspace_manifests WHERE id=:id"), {"id": source["manifest_id"]})).one()
        source_root = nas.nas_root / row.nas_prefix
    original_bytes = (source_root / "outputs/parent.txt").read_bytes()
    original_manifest = (source_root / "manifest.json").read_bytes()
    try:
        threads.get_paths = lambda: paths
        async with make_checkpointer(native_private) as saver:
            app.state.checkpointer = saver
            accessor, config = services.build_checkpoint_state_accessor(req, thread_id=original.thread_id)
            config["configurable"]["checkpoint_id"] = source["checkpoint_id"]
            accepted_state = await accessor.aget(config)
            inherited = [message for message in accepted_state.values["messages"] if getattr(message, "type", None) == "tool"]
            assert len([message for message in inherited if message.name == "present_files"]) == 2
            source_tuple = await saver.aget_tuple(config)
            assert source_tuple.metadata["deerflow_execution_run_id"] == original.run_id
            target_thread = original.thread_id
            if kind == "branch":
                assistant = next(message for message in reversed(accepted_state.values["messages"]) if message.type == "ai" and not message.tool_calls)
                csrf_token = generate_csrf_token()
                branch_headers = create_internal_auth_headers(owner_user_id=user.id) | {CSRF_HEADER_NAME: csrf_token}
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=branch_headers, cookies={CSRF_COOKIE_NAME: csrf_token}) as http:
                    response = await http.post(f"/api/threads/{original.thread_id}/branches", json={"message_id": assistant.id})
                assert response.status_code == 200, response.text
                target_thread = response.json()["thread_id"]
                clone = paths.sandbox_user_data_dir(target_thread, user_id=user.id)
                assert (clone / "outputs/parent.txt").read_bytes() == original_bytes
                assert (clone / "outputs/parent.txt").stat().st_ino != (source_root / "outputs/parent.txt").stat().st_ino
                async with db.engine.connect() as conn:
                    origin = (await conn.execute(text("SELECT source_workspace FROM thread_execution_bindings WHERE thread_id=:thread"), {"thread": target_thread})).one()
                assert origin.source_workspace["manifest_id"] == source["manifest_id"]
                assert origin.source_workspace["source_checkpoint_id"] == source["checkpoint_id"]
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=create_internal_auth_headers(owner_user_id=user.id)) as http:
                    preview = await http.get(f"/api/threads/{target_thread}/artifacts/mnt/user-data/outputs/parent.txt")
                assert preview.status_code == 200 and preview.content == original_bytes
            body = RunCreateRequest(
                input={"messages": [{"role": "user", "content": "c08-continuation=" + kind}]},
                config={"recursion_limit": 1000, "context": {"model_name": "model-1", "subagent_enabled": True}},
                stream_mode=["values", "messages-tuple", "custom"],
                stream_subgraphs=True,
            )
            record = await services.start_run(body, target_thread, req, execution_backend=backend)
            # The first run's join permanently closed its original publisher.
            # A new execution gets a fresh host publisher, with the same durable
            # journal and Node session; never reopen the old resource.
            stager = AgentWorkspacePublication(client=daemon.client, containers=driver, nas=stager.nas, journal=daemon.journal)
            daemon.workspace_publications = stager
            driver.provider = "c08-continuation"
            if kind in {"manifest-delete", "digest-mutation"}:
                if kind == "manifest-delete":
                    (source_root / "manifest.json").unlink()
                else:
                    (source_root / "outputs/parent.txt").write_bytes(b"mutated accepted artifact")
                starts_before = len(driver.ready)
                with __import__("pytest").raises(httpx.HTTPStatusError) as rejected:
                    await daemon.execute_one()
                assert rejected.value.response.status_code == 409
                async with db.engine.connect() as conn:
                    recovery = dict(
                        (
                            await conn.execute(
                                text(
                                    "SELECT t.state AS task_state,p.state AS placement_state,a.start_authorized_at,a.started_at,a.process_ref "
                                    "FROM fleet_run_placements p JOIN fleet_agent_tasks t ON t.id=p.agent_task_id JOIN fleet_attempts a ON a.id=p.active_attempt_id WHERE p.run_id=:run"
                                ),
                                {"run": record.run_id},
                            )
                        )
                        .mappings()
                        .one()
                    )
                assert recovery == {"task_state": "recovery_required", "placement_state": "recovery_required", "start_authorized_at": None, "started_at": None, "process_ref": None}
                assert len(driver.ready) == starts_before
                (evidence / (kind + "-rejected-start-proof.json")).write_text(
                    json.dumps(
                        {
                            "kind": kind,
                            "source_point": source,
                            "new_run_id": record.run_id,
                            "actual_http_status": rejected.value.response.status_code,
                            "recovery": recovery,
                            "starts_before": starts_before,
                            "starts_after": len(driver.ready),
                            "mismatched_restore_allowed": rejected.value.response.status_code != 409 or recovery["task_state"] != "recovery_required",
                        },
                        indent=2,
                    )
                )
                return
            run_task = asyncio.create_task(daemon.execute_one())
            deadline = time.monotonic() + 260
            while not run_task.done() and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            assert run_task.done(), "Actual continuation Runner deadline elapsed"
            result = await run_task
            assert result["state"] == "succeeded" and not result["report_pending"]
            await stager.join_writers()
            records = daemon.journal.records()
            owned = [item for item in records if item["grant"]["launch_spec"]["run_id"] == record.run_id]
            assert len(owned) == 1
            ref = "fleet-" + owned[0]["claim"]["attempt_id"]
            child_refs.append(ref)
            stopped = await driver.inspect(ref)
            assert not stopped["State"]["Running"] and stopped["State"]["ExitCode"] == 0
            _, stdout, stderr = await driver.command("logs", ref)
            from app.fleet.runner_context import control_connection_credentials
            from deerflow.config.app_config import AppConfig

            raw_log = stdout + stderr
            for secret in sorted(control_connection_credentials(AppConfig.model_validate(driver.operator_config)), key=len, reverse=True):
                raw_log = raw_log.replace(secret, "[private control credential redacted]")
            (evidence / (kind + "-runner.log")).write_text(raw_log)
            receipts = []
            for line in stdout.splitlines():
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if isinstance(value, dict) and "event" in value:
                    receipts.append(value)
            model = [row for row in receipts if row["event"] == "original-continuation-materialized-model-input"]
            assert len(model) == 3 and model[0]["run_id"] == record.run_id
            prior_ids = {message.tool_call_id for message in inherited}
            assert prior_ids <= {message["tool_call_id"] for message in model[0]["messages"] if message["type"] == "tool"}
            assert [[call["name"] for call in row["calls"]] for row in model] == [["bash"], ["present_files"], []]
            stamps = [row for row in receipts if row["event"] == "original-continuation-current-run-checkpoint"]
            assert stamps and all(row["stored_run_stamp"] == record.run_id != original.run_id for row in stamps)
            async with db.engine.connect() as conn:
                child_points = [dict(row) for row in (await conn.execute(text("SELECT id,checkpoint_id,manifest_id,kind FROM fleet_workspace_points WHERE run_id=:run ORDER BY accepted_at"), {"run": record.run_id})).mappings()]
            assert [row["kind"] for row in child_points] == ["partial", "final"]
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers=create_internal_auth_headers(owner_user_id=user.id)) as http:
                current_preview = await http.get(f"/api/threads/{target_thread}/artifacts/mnt/user-data/outputs/parent.txt")
                historical_preview = await http.get(f"/api/threads/{target_thread}/artifacts/mnt/user-data/outputs/parent.txt", params={"workspace_point_id": source["id"]})
                current_metadata = await http.get(f"/api/threads/{target_thread}/fleet/manifests/{child_points[-1]['manifest_id']}")
            assert current_preview.status_code == 200 and current_preview.content == original_bytes + ("continuation-" + kind + "\n").encode()
            assert historical_preview.status_code == 200 and historical_preview.content == original_bytes
            assert current_metadata.status_code == 200 and current_metadata.json()["point_id"] == child_points[-1]["id"]
            (evidence / (kind + "-artifact-http.json")).write_text(
                json.dumps({"default_status": current_preview.status_code, "historical_status": historical_preview.status_code, "metadata_status": current_metadata.status_code, "metadata": current_metadata.json()}, indent=2)
            )
            assert (source_root / "outputs/parent.txt").read_bytes() == original_bytes
            assert (source_root / "manifest.json").read_bytes() == original_manifest
            unchanged = await saver.aget_tuple(config)
            assert unchanged.metadata == source_tuple.metadata and unchanged.checkpoint == source_tuple.checkpoint
            (evidence / (kind + "-sequence-proof.json")).write_text(
                json.dumps(
                    {
                        "kind": kind,
                        "source_run_id": original.run_id,
                        "source_point": source,
                        "new_run_id": record.run_id,
                        "target_thread_id": target_thread,
                        "source_tool_ids": sorted(prior_ids),
                        "points": child_points,
                        "runner_receipts": receipts,
                        "docker_physical_stop": {"Id": stopped["Id"], "Image": stopped["Image"], "State": stopped["State"]},
                    },
                    indent=2,
                )
            )
    finally:
        import sys

        from .c08_installed_cleanup import settle_owned_containers

        original_error = sys.exception()

        def restore_paths():
            threads.get_paths = original_paths

        def discover_child():
            if record is None:
                return []
            found = []
            for item in daemon.journal.records():
                claim, grant = item.get("claim", {}), item.get("grant", {})
                attempt = claim.get("attempt_id")
                if (
                    claim.get("kind") == "agent"
                    and claim.get("run_id") == record.run_id
                    and grant.get("launch_spec", {}).get("run_id") == record.run_id
                    and attempt
                    and grant.get("attempt_id") == attempt
                    and grant.get("process_ref") == "fleet-" + attempt
                ):
                    found.append(grant["process_ref"])
            return found

        await settle_owned_containers(refs=child_refs, discover=discover_child, driver=driver, execution=run_task, before=[("restore-paths", restore_paths)], after=[("writers", stager.join_writers)], original_error=original_error)
