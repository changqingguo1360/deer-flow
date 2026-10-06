"""Production recipe, full Gateway lifespan and stock installed remote main."""

import asyncio
import json
import os
from pathlib import Path

import httpx
import pytest
from sqlalchemy import text


@pytest.mark.live
@pytest.mark.asyncio
async def test_c12_production_gateway_runner_main(tmp_path, monkeypatch):
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication
    from deerflow_ecs_fleet.workspace import NASWorkspace

    from deerflow.config import paths

    from .c12_integration_fixture import deterministic_provider, durable_state, login, normal_gateway, owned_database, private_configuration, response_json
    from .test_c01_remote_agent_admission import c_config

    evidence = Path(os.environ["C12_EVIDENCE_DIR"])
    evidence.mkdir(parents=True, exist_ok=True)
    image = os.environ["FLEET_AGENT_TEST_IMAGE"]
    monkeypatch.setenv("AUTH_JWT_SECRET", "c12-private-isolated-jwt-key-" + tmp_path.name)
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(paths, "_paths", paths.Paths(tmp_path / "home"))
    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("fleet-test\n")
    email, password = "c12-admin@example.com", "C12-Isolated-Password-4827"
    async with owned_database(evidence) as db, deterministic_provider(evidence) as (provider_url, calls):
        private = private_configuration(db, provider_url)
        driver = AgentContainers(state_dir=tmp_path / "node", operator_config=private, provider="gateway", executable=os.environ["C12_DOCKER"])
        original_stop = driver.stop

        async def observed_stop(ref):
            try:
                _, stdout, stderr = await driver.command("logs", ref, timeout=10)
                output = stdout + stderr
                for value in (db.runner_url, db.host_url, password, private["models"][0]["api_key"]):
                    output = output.replace(value, "[private redacted]")
                (evidence / "runner-before-stop.log").write_text(output)
            finally:
                result = await original_stop(ref)
            return result

        driver.stop = observed_stop
        actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
        (evidence / "actual-installed-compatibility.json").write_text(actual.model_dump_json(indent=2))
        host = {**private, "database": {**private["database"], "postgres_url": db.host_url}, "models": [{**private["models"][0], "base_url": provider_url + "/v1"}]}
        async with normal_gateway(host) as (_, client, _):
            user = await response_json(client, "POST", "/api/v1/auth/initialize", json={"email": email, "password": password})
        fleet = c_config()
        fleet.update(nas_root=str(nas), nas_identity="fleet-test")
        fleet["profiles"]["remote"].update(image=image, runtime_digest=actual.runtime_digest, user=str(os.getuid()) + ":" + str(os.getgid()), network="bridge", pids_limit=256, execution_timeout_seconds=240)
        fleet["agent_bindings"] = {
            "remote": {
                "allowed_user_ids": [user["id"]],
                "model_name": "model-1",
                "model_version": "c12-v1",
                "compatibility": actual.model_dump(mode="json"),
                "secret_refs": [{"name": "MODEL_API_KEY", "reference_id": "operator-model-binding"}],
            }
        }
        host["plugins"] = [{"use": "deerflow_ecs_fleet:install", "required": True, "table_prefix": "fleet_", "config": fleet}]
        async with normal_gateway(host) as (app, client, url):
            await login(client, email, password)
            node_id = "c12-node-" + __import__("uuid").uuid4().hex
            await response_json(client, "POST", "/api/fleet/machines", json={"node_id": node_id, "name": node_id, "cpu_millis": 1000, "memory_mib": 2048, "agent_limit": 1, "profile_allowlist": ["remote"]})
            credential = await response_json(client, "POST", f"/api/fleet/machines/{node_id}/credentials", json={"lifetime_seconds": 600})
            thread = await response_json(client, "POST", "/api/threads", json={"thread_id": "c12-remote"})
            body = {
                "input": {"messages": [{"role": "user", "content": "Create result.txt with the deterministic artifact and present it."}]},
                "config": {"recursion_limit": 1000, "context": {"model_name": "model-1", "subagent_enabled": False}},
                "stream_mode": ["values", "messages-tuple", "custom"],
                "execution": {"preference": "remote", "profile": "remote"},
            }
            run = await response_json(client, "POST", "/api/threads/c12-remote/runs", json=body)
            (evidence / "live-handle.json").write_text(json.dumps({"image": image, "schema": db.schema, "node_id": node_id, "thread_id": thread["thread_id"], "run_id": run["run_id"], "state": "admitted"}, indent=2))
            async with httpx.AsyncClient(base_url=url + "/", timeout=15) as node_http:
                node = NodeClient(gateway_url=url, credential=credential["token"], claim_kind="agent", compatibility=actual.model_dump(mode="json"), http_client=node_http)

                async def prepare(claim, grant):
                    return await driver.prepare_workspace(nas, claim, grant)

                daemon = NodeDaemon(client=node, containers=driver, state_dir=tmp_path / "node", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.25, poll_seconds=0.05)
                stager = AgentWorkspacePublication(client=node, containers=driver, nas=NASWorkspace(nas, identity="fleet-test"), journal=daemon.journal)
                daemon.workspace_publications = stager
                await daemon.bootstrap()
                execution = asyncio.create_task(daemon.execute_one())
                refs = []
                try:
                    async with asyncio.timeout(160):
                        result = await asyncio.shield(execution)
                    await stager.join_writers()
                    refs = [ref for ref, _ in await driver.list_managed(node_id)]
                    assert len(refs) == 1
                    observation = await driver.inspect(refs[0])
                    _, stdout, stderr = await driver.command("logs", refs[0], timeout=10)
                    output = stdout + stderr
                    for value in (db.runner_url, db.host_url, password, credential["token"], private["models"][0]["api_key"]):
                        output = output.replace(value, "[private redacted]")
                    (evidence / "runner.log").write_text(output)
                    assert not observation["State"]["Running"] and observation["State"]["ExitCode"] == 0, output
                    assert observation["Image"] == image and result["stop_reason"] == "exit" and not result["report_pending"]
                    state = await durable_state(db, run["run_id"])
                    assert state["status"] == "success", (state, output)
                    assert state["reservation"] == "released" and state["stopped_at"] is not None
                    assert state["accepted_workspace_point_id"] == state["final_workspace_point_id"] and state["accepted_workspace_point_id"]
                    artifact = await client.get("/api/threads/c12-remote/artifacts/mnt/user-data/outputs/result.txt")
                    assert artifact.status_code == 200 and artifact.content == b"c12-original-artifact\n", artifact.text
                    sse = await client.get(f"/api/threads/c12-remote/runs/{run['run_id']}/join")
                    assert sse.status_code == 200 and "event: end" in sse.text.lower(), sse.text[-1500:]
                    (evidence / "durable-tail.sse").write_text(sse.text)
                    checkpoint = await app.state.checkpointer.aget_tuple({"configurable": {"thread_id": "c12-remote"}})
                    assert checkpoint and "Completed the deterministic artifact." in str(checkpoint.checkpoint["channel_values"]["messages"])
                    async with db.session_factory() as session:
                        assert await session.scalar(text("SELECT count(*) FROM fleet_event_outbox WHERE run_id=:run"), {"run": run["run_id"]}) > 0
                    remote_calls = list(calls)
                    await response_json(client, "POST", "/api/threads", json={"thread_id": "c12-local"})
                    local = await response_json(client, "POST", "/api/threads/c12-local/runs/wait", json={**body, "execution": {"preference": "local"}})
                    assert len(calls) == 6 and [row["chosen_tool"] for row in calls[:3]] == [row["chosen_tool"] for row in calls[3:]]
                    local_artifact = await client.get("/api/threads/c12-local/artifacts/mnt/user-data/outputs/result.txt")
                    assert local_artifact.content == artifact.content
                    (evidence / "main-observations.json").write_text(
                        json.dumps(
                            {
                                "image": image,
                                "actual_compatibility": actual.model_dump(mode="json"),
                                "state": state,
                                "docker_state": observation["State"],
                                "node_result": result,
                                "remote_provider_calls": remote_calls,
                                "local_provider_calls": calls[3:],
                                "local_response": local,
                                "artifact_sha256": __import__("hashlib").sha256(artifact.content).hexdigest(),
                            },
                            indent=2,
                            default=str,
                        )
                    )
                finally:
                    import sys

                    from .c08_installed_cleanup import settle_owned_containers

                    async def collect_logs():
                        for ref in await discover(driver, node_id):
                            if ref not in refs:
                                refs.append(ref)
                            _, stdout, stderr = await driver.command("logs", ref, timeout=10)
                            output = stdout + stderr
                            for value in (db.runner_url, db.host_url, password, credential["token"], private["models"][0]["api_key"]):
                                output = output.replace(value, "[private redacted]")
                            (evidence / "runner.log").write_text(output)

                    await settle_owned_containers(
                        refs=refs, discover=lambda: discover(driver, node_id), driver=driver, execution=execution, before=[("runner-logs", collect_logs)], after=[("workspace-writers", stager.join_writers)], original_error=sys.exc_info()[1]
                    )


async def discover(driver, node_id):
    return [ref for ref, _ in await driver.list_managed(node_id)]


@pytest.mark.live
@pytest.mark.asyncio
async def test_c12_production_runner_revocation_and_delivery_recovery(tmp_path, monkeypatch):
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.daemon import NodeDaemon
    from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication
    from deerflow_ecs_fleet.workspace import NASWorkspace

    from deerflow.config import paths

    from .c07_integration_fixture import RedisFaultProxy, owned_redis
    from .c12_integration_fixture import deterministic_provider, durable_state, installed_late_checkpoint_probe, login, normal_gateway, owned_database, private_configuration, response_json, start_stock_node, table_fingerprints
    from .test_c01_remote_agent_admission import c_config

    evidence = Path(os.environ["C12_EVIDENCE_DIR"])
    evidence.mkdir(parents=True, exist_ok=True)
    image, docker = os.environ["FLEET_AGENT_TEST_IMAGE"], os.environ["C12_DOCKER"]
    monkeypatch.setenv("AUTH_JWT_SECRET", "c12-private-isolated-jwt-key-" + tmp_path.name)
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(paths, "_paths", paths.Paths(tmp_path / "home"))
    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("fleet-test\n")
    email, password = "c12-admin@example.com", "C12-Isolated-Password-4827"
    control = {"delivery_waiting": asyncio.Event(), "delivery_release": asyncio.Event()}
    async with owned_database(evidence) as db, deterministic_provider(evidence, control=control) as (provider_url, _), owned_redis(tmp_path / "redis") as (redis, redis_port, _):
        proxy = RedisFaultProxy(redis_port)
        await proxy.start()
        try:
            private = private_configuration(db, provider_url)
            driver = AgentContainers(state_dir=tmp_path / "node", operator_config=private, provider="gateway", executable=docker)
            original_stop = driver.stop

            async def stop_with_logs(ref):
                try:
                    _, stdout, stderr = await driver.command("logs", ref, timeout=10)
                    output = stdout + stderr
                    for secret in (db.host_url, db.runner_url, password, private["models"][0]["api_key"]):
                        output = output.replace(secret, "[private redacted]")
                    (evidence / (ref + "-runner.log")).write_text(output)
                except Exception:
                    pass
                return await original_stop(ref)

            driver.stop = stop_with_logs
            actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
            host = {**private, "database": {**private["database"], "postgres_url": db.host_url}, "models": [{**private["models"][0], "base_url": provider_url + "/v1"}]}
            async with normal_gateway(host) as (_, client, _):
                user = await response_json(client, "POST", "/api/v1/auth/initialize", json={"email": email, "password": password})
            fleet = c_config()
            fleet.update(nas_root=str(nas), nas_identity="fleet-test")
            fleet["profiles"]["remote"].update(image=image, runtime_digest=actual.runtime_digest, user=str(os.getuid()) + ":" + str(os.getgid()), network="bridge", pids_limit=256, execution_timeout_seconds=240)
            fleet["agent_bindings"] = {
                "remote": {
                    "allowed_user_ids": [user["id"]],
                    "model_name": "model-1",
                    "model_version": "c12-v1",
                    "compatibility": actual.model_dump(mode="json"),
                    "secret_refs": [{"name": "MODEL_API_KEY", "reference_id": "operator-model-binding"}],
                }
            }
            host["plugins"] = [{"use": "deerflow_ecs_fleet:install", "required": True, "table_prefix": "fleet_", "config": fleet}]
            host["stream_bridge"] = {"type": "redis", "redis_url": f"redis://127.0.0.1:{proxy.port}/0"}
            async with normal_gateway(host) as (app, client, url):
                await login(client, email, password)
                node_id = "c12-fault-" + __import__("uuid").uuid4().hex
                await response_json(client, "POST", "/api/fleet/machines", json={"node_id": node_id, "name": node_id, "cpu_millis": 1000, "memory_mib": 2048, "agent_limit": 1, "profile_allowlist": ["remote"]})
                credential = await response_json(client, "POST", f"/api/fleet/machines/{node_id}/credentials", json={"lifetime_seconds": 600})
                body = {
                    "input": {"messages": [{"role": "user", "content": "Create and present the deterministic artifact."}]},
                    "config": {"recursion_limit": 1000, "context": {"model_name": "model-1", "subagent_enabled": False}},
                    "stream_mode": ["values", "messages-tuple", "custom"],
                    "execution": {"preference": "remote", "profile": "remote"},
                }
                await response_json(client, "POST", "/api/threads", json={"thread_id": "c12-delivery"})
                delivered_run = await response_json(client, "POST", "/api/threads/c12-delivery/runs", json=body)
                async with httpx.AsyncClient(base_url=url + "/", timeout=15) as node_http:
                    node = NodeClient(gateway_url=url, credential=credential["token"], claim_kind="agent", compatibility=actual.model_dump(mode="json"), http_client=node_http)

                    async def prepare(claim, grant):
                        return await driver.prepare_workspace(nas, claim, grant)

                    daemon = NodeDaemon(client=node, containers=driver, state_dir=tmp_path / "node", prepare_workspace=prepare, renew_seconds=1, safety_margin_seconds=0.25, poll_seconds=0.05)
                    stager = AgentWorkspacePublication(client=node, containers=driver, nas=NASWorkspace(nas, identity="fleet-test"), journal=daemon.journal)
                    daemon.workspace_publications = stager
                    await daemon.bootstrap()
                    execution = asyncio.create_task(daemon.execute_one())
                    refs = []
                    try:
                        delivery_wait = asyncio.create_task(control["delivery_waiting"].wait())
                        try:
                            completed, _ = await asyncio.wait((delivery_wait, execution), timeout=60, return_when=asyncio.FIRST_COMPLETED)
                            assert completed, "Original delivery event and execution still pending"
                            if delivery_wait not in completed:
                                early = await execution
                                raise AssertionError(("Original execution ended before delivery barrier", early))
                        finally:
                            delivery_wait.cancel()
                            await asyncio.gather(delivery_wait, return_exceptions=True)
                        async with db.session_factory() as session:
                            committed_before = await session.scalar(text("SELECT count(*) FROM fleet_event_outbox WHERE run_id=:run"), {"run": delivered_run["run_id"]})
                        assert committed_before > 0
                        await proxy.disconnect()
                        control["delivery_release"].set()
                        result = await asyncio.wait_for(asyncio.shield(execution), 160)
                        assert result["state"] == "succeeded" and not result["report_pending"]
                        state = await durable_state(db, delivered_run["run_id"])
                        assert state["status"] == "success" and state["reservation"] == "released"
                        tail = await client.get(f"/api/threads/c12-delivery/runs/{delivered_run['run_id']}/join")
                        assert tail.status_code == 200 and "event: end" in tail.text.lower()
                        (evidence / "delivery-offline-durable-tail.sse").write_text(tail.text)
                        async with db.session_factory() as session:
                            missing = await session.scalar(text("SELECT count(*) FROM fleet_event_outbox WHERE run_id=:run AND published_at IS NULL"), {"run": delivered_run["run_id"]})
                            attempt = await session.scalar(text("SELECT active_attempt_id FROM fleet_run_placements WHERE run_id=:run"), {"run": delivered_run["run_id"]})
                        assert missing > 0
                        publisher = app.state.stream_bridge.publisher
                        await publisher.stop()
                        proxy.available = True
                        await publisher.start()
                        async with asyncio.timeout(30):
                            while True:
                                async with db.session_factory() as session:
                                    pending = await session.scalar(text("SELECT count(*) FROM fleet_event_outbox WHERE run_id=:run AND published_at IS NULL"), {"run": delivered_run["run_id"]})
                                if not pending:
                                    break
                                await asyncio.sleep(0.05)
                        async with db.session_factory() as session:
                            committed_seqs = list((await session.execute(text("SELECT o.seq FROM run_events e JOIN fleet_event_outbox o ON o.event_id=e.id WHERE o.run_id=:run ORDER BY o.seq"), {"run": delivered_run["run_id"]})).scalars())
                        hints = await redis.xrange(publisher.key_prefix + ":" + delivered_run["run_id"] + ":" + attempt)
                        ids = [row[0].decode() for row in hints]
                        assert ids == [str(seq) + "-0" for seq in committed_seqs]
                        assert ids and len(ids) == len(set(ids)) and ids == sorted(ids, key=lambda item: int(item.split("-")[0]))
                        (evidence / "delivery-recovery.json").write_text(
                            json.dumps(
                                {"run_id": delivered_run["run_id"], "state": state, "committed_before_cutoff": committed_before, "unpublished_during_outage": missing, "hint_ids": ids, "resp": proxy.command_receipts}, indent=2, default=str
                            )
                        )
                    finally:
                        from .c08_installed_cleanup import settle_owned_containers

                        control["delivery_release"].set()
                        await settle_owned_containers(refs=refs, discover=lambda: discover(driver, node_id), driver=driver, execution=execution, after=[("workspace-writers", stager.join_writers)])
                # A second owned occurrence has the distinct running/revocation lifecycle.
                await response_json(client, "POST", "/api/threads", json={"thread_id": "c12-revoke"})
                revoked_run = await response_json(client, "POST", "/api/threads/c12-revoke/runs", json={**body, "input": {"messages": [{"role": "user", "content": "c12-revoke: create, present then hold the original supervised shell."}]}})
                stock_dir = tmp_path / "stock"
                process, log = await start_stock_node(directory=stock_dir, url=url, credential=credential["token"], private=private, image=image, nas=nas, evidence=evidence / "stock-first.log")
                restarted = restart_log = None
                refs = []
                try:
                    async with asyncio.timeout(60):
                        while True:
                            refs = await discover(driver, node_id)
                            if refs:
                                if list(nas.rglob("c12-revoke-shell-start")):
                                    break
                            assert process.returncode is None, "Original stock Node exited before actual supervised shell"
                            await asyncio.sleep(0.05)
                    before = await durable_state(db, revoked_run["run_id"])
                    assert before["status"] == "running" and before["reservation"] != "released" and before["accepted_workspace_point_id"]
                    journal = next(row for row in (json.loads(path.read_text()) for path in (stock_dir / "journal").glob("*.json")) if row["claim"].get("run_id") == revoked_run["run_id"])
                    checkpoint = await app.state.checkpointer.aget_tuple({"configurable": {"thread_id": "c12-revoke"}})
                    process.kill()
                    assert await process.wait() == -9
                    log.close()
                    revoked = await client.delete(f"/api/fleet/machines/{node_id}/credentials/{credential['credential_id']}")
                    assert revoked.status_code == 204
                    fresh = await response_json(client, "POST", f"/api/fleet/machines/{node_id}/credentials", json={"lifetime_seconds": 600})
                    async with httpx.AsyncClient(base_url=url + "/", timeout=15) as rotated_http:
                        rotated = NodeClient(gateway_url=url, credential=fresh["token"], claim_kind="agent", compatibility=actual.model_dump(mode="json"), http_client=rotated_http)
                        await rotated.open_session()
                    after_rotation = await durable_state(db, revoked_run["run_id"])

                    async def delivery_snapshot():
                        async with db.session_factory() as session:
                            rows = (await session.execute(text("SELECT event_id,published_at FROM fleet_event_outbox ORDER BY event_id"))).all()
                            return [{"event_id": str(row[0]), "published_at": str(row[1])} for row in rows]

                    delivery_before_probe = await delivery_snapshot()
                    fingerprints_before = await table_fingerprints(db)
                    rejection = await installed_late_checkpoint_probe(docker=docker, image=image, private=private, journal=journal, checkpoint=checkpoint.config)
                    assert rejection["rejected"]
                    (evidence / "installed-source-audit.json").write_text(json.dumps(rejection.pop("installed_audit"), indent=2))
                    fingerprints_after = await table_fingerprints(db)
                    delivery_after_probe = await delivery_snapshot()
                    assert fingerprints_after == fingerprints_before

                    async def process_rows():
                        async with db.session_factory() as session:
                            return list((await session.execute(text("SELECT row_to_json(p)::text FROM fleet_workspace_processes p WHERE attempt_id=:attempt ORDER BY pid"), {"attempt": journal["claim"]["attempt_id"]})).scalars())

                    restart_before = {"docker": (await driver.inspect(refs[0]))["State"], "process_rows": await process_rows(), "durable": await durable_state(db, revoked_run["run_id"])}
                    restarted, restart_log = await start_stock_node(directory=stock_dir, url=url, credential=fresh["token"], private=private, image=image, nas=nas, evidence=evidence / "stock-restart.log")
                    async with asyncio.timeout(45):
                        while True:
                            stopped = await driver.inspect(refs[0])
                            final = await durable_state(db, revoked_run["run_id"])
                            if stopped is None or not stopped["State"]["Running"]:
                                if final["reservation"] == "released" and final["stopped_at"] is not None:
                                    break
                            if restarted.returncode is not None:
                                raise AssertionError("Stock journal restart exited before STOP/release")
                            await asyncio.sleep(0.05)
                    final_journal = json.loads((stock_dir / "journal" / (journal["claim"]["attempt_id"] + ".json")).read_text())
                    journal_observed = {key: value for key, value in final_journal.items() if key in {"state", "phase", "stop_reason", "exit_code", "process_ref", "acknowledged", "stopped", "physical_stopped"}}
                    (evidence / "revocation-observations.json").write_text(
                        json.dumps(
                            {
                                "run_id": revoked_run["run_id"],
                                "original_attempt_id": journal["claim"]["attempt_id"],
                                "original_node_pid": process.pid,
                                "original_node_exit": process.returncode,
                                "before": before,
                                "after_authorized_rotation": after_rotation,
                                "delivery_before_probe": delivery_before_probe,
                                "delivery_after_probe": delivery_after_probe,
                                "fingerprint_scope": "Eight mutation tables; outbox excludes only publisher-owned published_at",
                                "final": final,
                                "late_write_probe": rejection,
                                "rows_before_probe": fingerprints_before,
                                "rows_after_probe": fingerprints_after,
                                "physical_state": stopped["State"] if stopped else "removed",
                                "restart_pid": restarted.pid,
                                "restart_before": restart_before,
                                "process_rows_after_restart": await process_rows(),
                                "journal_after_restart": journal_observed,
                                "journal_observed_keys": sorted(final_journal),
                                "end_scope": "Ownership loss does not manufacture END",
                                "memory_store_scope": "stateless noop; no actual Store mutation",
                            },
                            indent=2,
                            default=str,
                        )
                    )
                finally:
                    import sys

                    from .c08_installed_cleanup import settle_owned_cleanup

                    async def stop_process(owner):
                        if owner is not None and owner.returncode is None:
                            owner.terminate()
                            try:
                                await asyncio.wait_for(owner.wait(), 15)
                            except TimeoutError:
                                owner.kill()
                                await owner.wait()

                    async def collect_logs():
                        refs[:] = await discover(driver, node_id)
                        for ref in refs:
                            _, stdout, stderr = await driver.command("logs", ref, timeout=10)
                            output = stdout + stderr
                            for value in (db.runner_url, db.host_url, password, credential["token"], private["models"][0]["api_key"]):
                                output = output.replace(value, "[private redacted]")
                            (evidence / "revoked-runner.log").write_text(output)

                    async def stop_refs():
                        await settle_owned_cleanup(
                            [("container-stop", lambda ref=ref: driver.stop(ref)) for ref in refs],
                            original_error=sys.exception(),
                        )

                    async def remove_refs():
                        await settle_owned_cleanup(
                            [("container-remove", lambda ref=ref: driver.command("rm", "--force", ref, timeout=30)) for ref in refs],
                            original_error=sys.exception(),
                        )

                    actions = [("logs", collect_logs)]
                    actions.extend(("stock-process", lambda owner=owner: stop_process(owner)) for owner in (process, restarted))
                    for output in (log, restart_log):
                        if output is not None:
                            actions.append(("stock-log", output.close))
                    actions.extend([("container-stop", stop_refs), ("container-remove", remove_refs)])
                    await settle_owned_cleanup(actions, original_error=sys.exception())
        finally:
            await proxy.close()
