"""One installed stock two-node C→B→C goal; real UI stays inside its lifespan."""

import asyncio
import hashlib
import json
import os
import secrets
import socket
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from .c04_integration_fixture import node_server
from .c07_integration_fixture import owned_redis
from .c12_integration_fixture import login, normal_gateway, owned_database, private_configuration, response_json, start_stock_node
from .test_bc_acceptance import collect_main

ROOT = Path(__file__).resolve().parents[3]
TABLES = (
    "fleet_agent_tasks",
    "runs",
    "fleet_run_placements",
    "fleet_attempts",
    "fleet_reservations",
    "fleet_nodes",
    "fleet_wait_groups",
    "fleet_job_links",
    "fleet_jobs",
    "fleet_artifact_manifests",
    "fleet_workspace_points",
    "fleet_workspace_manifests",
    "fleet_workspace_processes",
    "checkpoints",
    "checkpoint_writes",
    "run_events",
    "fleet_event_outbox",
)


def save(directory, name, payload):
    path = directory / name
    path.write_text(json.dumps(payload, default=str, indent=2))
    path.chmod(0o600)


async def bounded_wait(predicate, *, seconds, processes=()):
    async with asyncio.timeout(seconds):
        while True:
            result = await predicate()
            if result:
                return result
            if any(process.returncode is not None for process in processes):
                raise AssertionError("An original owned runtime exited before the expected boundary")
            await asyncio.sleep(0.1)


@asynccontextmanager
async def original_child_provider(evidence):
    """Script only model responses and original job argv; no substitute graph."""
    app = FastAPI()
    control = {"child_started": asyncio.Event(), "child_release": asyncio.Event(), "url": None, "browser_token": secrets.token_hex(24)}
    calls, child_receipts = [], []

    @app.post("/child/barrier")
    async def child_barrier(request: Request):
        receipt = await request.json()
        child_receipts.append(receipt)
        save(evidence, "child-http-receipts.json", child_receipts)
        control["child_started"].set()
        await control["child_release"].wait()
        return {"released": True}

    @app.post("/browser/release")
    async def release(request: Request):
        body = await request.json()
        if body.get("token") != control["browser_token"]:
            from fastapi import HTTPException

            raise HTTPException(status_code=403)
        control["child_release"].set()
        return {"released_original_child": True}

    @app.post("/v1/chat/completions")
    async def complete(request: Request):
        body = await request.json()
        messages = body["messages"]
        continued = any("The awaited background jobs have settled." in str(message.get("content")) for message in messages)
        tools = [message for message in messages if message["role"] == "tool"]
        call = None
        observed_results = []
        for message in messages:
            content = message.get("content")
            if message["role"] != "user" or not isinstance(content, str) or "The awaited background jobs have settled." not in content:
                continue
            offset = content.index("The awaited background jobs have settled.")
            payload, _ = json.JSONDecoder().raw_decode(content[content.index("{", offset) :])
            if payload.get("schema_version") != 1 or not payload.get("continuation_key"):
                raise AssertionError("Original continuation payload identity missing")
            observed_results.extend(payload["results"])
        consumed = bool(observed_results) and all(row["state"] == "succeeded" and row["manifest_id"] and any(file["path"] == "child.txt" and file["sha256"] for file in row["files"]) for row in observed_results)
        if not continued:
            name = "submit_fleet_job" if not tools else "await_fleet_jobs"
            child_url = control["url"].replace("127.0.0.1", "host.docker.internal") + "/child/barrier"
            code = (
                "import json,os,urllib.request; from pathlib import Path; "
                "receipt={'logical_effect':'bc10-original-child-result','pid':os.getpid()}; "
                "f=Path('effects.jsonl').open('a'); f.write(json.dumps(receipt)+'\\n'); f.flush(); os.fsync(f.fileno()); f.close(); "
                "Path('child.txt').write_text('bc10-original-child result\\n'); "
                "urllib.request.urlopen(urllib.request.Request(" + repr(child_url) + ",data=json.dumps(receipt).encode(),headers={'Content-Type':'application/json'}),timeout=240).read()"
            )
            args = {"task_name": "bc10-original-child", "profile": "batch-standard", "argv": ["python", "-c", code], "link_mode": "awaited", "execution_timeout_seconds": 240, "queue_timeout_seconds": 240} if not tools else {}
            call = {"id": "bc10-" + name, "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}
        elif not consumed:
            raise AssertionError("Original continuation did not receive original accepted child result")
        calls.append(
            {
                "continued": continued,
                "tool_count": len(tools),
                "chosen_tool": call["function"]["name"] if call else None,
                "consumed_original_result": consumed,
                "observed_original_results": observed_results,
                "message_roles": [message["role"] for message in messages],
                "original_continuation_observations": [message["content"] for message in messages if "The awaited background jobs have settled." in str(message.get("content"))],
            }
        )
        save(evidence, "provider-observations.json", calls)
        message = {"role": "assistant", "content": "" if call else "The original bc10-original-child result was consumed and the same goal is complete."}
        if call:
            message["tool_calls"] = [call]
        common = {"id": "bc10-" + str(len(calls)), "created": 1791216000, "model": body["model"]}
        if not body.get("stream"):
            return common | {"object": "chat.completion", "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if call else "stop"}], "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}}

        async def chunks():
            delta = dict(message)
            if call:
                delta["tool_calls"] = [{"index": 0, **call}]
            for payload in (
                common | {"object": "chat.completion.chunk", "choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
                common | {"object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if call else "stop"}], "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
            ):
                yield "data: " + json.dumps(payload) + "\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(chunks(), media_type="text/event-stream")

    async with node_server(app) as url:
        control["url"] = url
        try:
            yield url, calls, control
        finally:
            control["child_release"].set()


async def snapshot(db, nas, driver, worker_refs, *, thread_id, calls, evidence):
    tables = {}
    async with db.engine.connect() as connection:
        for table in TABLES:
            rows = (await connection.execute(text("SELECT to_jsonb(t)-'token_hash' AS row FROM " + table + " t"))).scalars().all()
            tables[table] = rows
        clock = await connection.scalar(text("SELECT clock_timestamp()"))
    execution = {}
    outputs = []
    for attempt in tables["fleet_attempts"]:
        execution[attempt["id"]] = await driver.inspect(attempt["process_ref"] or "fleet-" + attempt["id"])
        if attempt["kind"] == "job":
            files = []
            effect_lines = []
            base = nas / attempt["output_prefix"]
            # Scan writable original output once; sealed is an immutable copy,
            # so counting both would count the same physical effect twice.
            for path in sorted(base.rglob("*")) if base.is_dir() else []:
                if not path.is_file() or "sealed" in path.relative_to(base).parts:
                    continue
                data = path.read_bytes()
                files.append({"path": str(path.relative_to(base)), "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)})
                if path.name == "effects.jsonl":
                    effect_lines.extend(data.decode().splitlines())
            outputs.append({"job_id": attempt["job_id"], "attempt_id": attempt["id"], "state": attempt["state"], "output_prefix": attempt["output_prefix"], "output_scanned": base.is_dir(), "files": files, "effect_lines": effect_lines})

    def redact_journal(value):
        if isinstance(value, dict):
            return {(key + "_sha256" if key == "token" else key): (hashlib.sha256(item.encode()).hexdigest() if key == "token" and isinstance(item, str) else redact_journal(item)) for key, item in value.items()}
        if isinstance(value, list):
            return [redact_journal(item) for item in value]
        return value

    journals = []
    for path in sorted(nas.parent.glob("stock-*/journal/*.json")):
        payload = path.read_bytes()
        journals.append({"file": str(path.relative_to(nas.parent)), "raw_sha256": hashlib.sha256(payload).hexdigest(), "mode": oct(path.stat().st_mode & 0o777), "original_record_redacted": redact_journal(json.loads(payload))})
    raw = {
        "thread_id": thread_id,
        "database_clock": clock,
        "tables": tables,
        "execution_containers": execution,
        "running_container_observations": json.loads((evidence / "running-container-observations.json").read_text()) if (evidence / "running-container-observations.json").is_file() else {},
        "worker_containers": [await driver.inspect(ref) for ref in worker_refs],
        "all_job_attempt_outputs": outputs,
        "provider_calls": list(calls),
        "original_journals": journals,
        "child_http_receipts": json.loads((evidence / "child-http-receipts.json").read_text()) if (evidence / "child-http-receipts.json").is_file() else [],
        "agent_image_id": os.environ["FLEET_AGENT_TEST_IMAGE"],
        "worker_image_id": os.environ["BC10_WORKER_IMAGE"],
    }
    save(evidence, "main-raw.json", raw)
    return raw


def local_worker_tls(directory):
    """Owned verified HTTPS transport for original stock Node credentials."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    directory.mkdir(mode=0o700)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "BC10 owned local Gateway")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(digital_signature=True, content_commitment=False, key_encipherment=True, data_encipherment=False, key_agreement=False, key_cert_sign=True, crl_sign=True, encipher_only=False, decipher_only=False), critical=True
        )
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("host.docker.internal"), x509.IPAddress(ip_address("127.0.0.1"))]), critical=False)
        .sign(key, hashes.SHA256())
    )
    ca_file, key_file = directory / "gateway-ca.pem", directory / "gateway-key.pem"
    ca_file.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    ca_file.chmod(0o600)
    key_file.chmod(0o600)
    return ca_file, key_file


@pytest.mark.live
@pytest.mark.asyncio
async def test_bc10_installed_two_stock_workers_main(tmp_path, monkeypatch):
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers

    from deerflow.config import paths

    from .test_c01_remote_agent_admission import c_config

    evidence = Path(os.environ["BC10_EVIDENCE_DIR"])
    evidence.mkdir(parents=True, exist_ok=True)
    image, worker_image, docker = os.environ["FLEET_AGENT_TEST_IMAGE"], os.environ["BC10_WORKER_IMAGE"], os.environ["C12_DOCKER"]
    for name in ("installed-source-audit.json",):
        assert (evidence / name).is_file(), "Exact fresh installed source qualification must precede this main"
    monkeypatch.setenv("AUTH_JWT_SECRET", "bc10-owned-key-" + tmp_path.name)
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(paths, "_paths", paths.Paths(tmp_path / "home"))
    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("fleet-test\n")
    email, password = "bc10-admin@example.com", secrets.token_urlsafe(24)
    thread_id = "bc10-" + secrets.token_hex(12)
    suffix = secrets.token_hex(10)
    node_ids = ["bc10-a-" + suffix, "bc10-b-" + suffix]
    worker_refs = ["bc10-worker-a-" + suffix, "bc10-worker-b-" + suffix]
    processes, logs, browser, next_process, observer = [], [], None, None, None
    retention = {"live": False}
    async with owned_database(evidence, retention=retention) as db, original_child_provider(evidence) as (provider_url, calls, control), owned_redis(tmp_path / "redis") as (_, redis_port, redis_pid):
        private = private_configuration(db, provider_url)
        private["models"][0]["max_completion_tokens"] = 4096
        private["stream_bridge"] = {"type": "redis", "redis_url": f"redis://host.docker.internal:{redis_port}/0"}
        driver = AgentContainers(state_dir=tmp_path / "observer", operator_config=private, provider="gateway", executable=docker)
        actual = WorkerCompatibility.model_validate(await driver.compatibility(image))
        host = {
            **private,
            "database": {**private["database"], "postgres_url": db.host_url},
            "models": [{**private["models"][0], "base_url": provider_url + "/v1"}],
            "stream_bridge": {"type": "redis", "redis_url": f"redis://127.0.0.1:{redis_port}/0"},
        }
        async with normal_gateway(host) as (_, client, _):
            user = await response_json(client, "POST", "/api/v1/auth/initialize", json={"email": email, "password": password})
        fleet = c_config()
        fleet.update(nas_root=str(nas), nas_identity="fleet-test", continuations_enabled=True, scheduling_mode="serial")
        for name in ("remote", "batch-standard"):
            fleet["profiles"][name].update(image=image, user=str(os.getuid()) + ":" + str(os.getgid()), network="bridge", execution_timeout_seconds=300)
        fleet["profiles"]["remote"].update(runtime_digest=actual.runtime_digest, pids_limit=256)
        fleet["agent_bindings"] = {
            "remote": {
                "allowed_user_ids": [user["id"]],
                "model_name": "model-1",
                "model_version": "c12-v1",
                "compatibility": actual.model_dump(mode="json"),
                "secret_refs": [{"name": "MODEL_API_KEY", "reference_id": "operator-model-binding"}],
                "continuation_budget": 2,
            }
        }
        plugin = {"name": "ecs-fleet", "package": "deerflow-ecs-fleet", "use": "deerflow_ecs_fleet:install", "required": True, "table_prefix": "fleet_", "config": fleet}
        private["plugins"] = host["plugins"] = [plugin]
        ca_file, key_file = local_worker_tls(tmp_path / "worker-tls")
        async with normal_gateway(host) as (app, client, url), node_server(app, ssl_certfile=str(ca_file), ssl_keyfile=str(key_file), lifecycle_evidence=evidence / "worker-tls-lifecycle.json") as worker_url:
            await login(client, email, password)
            try:
                for node_id in node_ids:
                    await response_json(client, "POST", "/api/fleet/machines", json={"node_id": node_id, "name": node_id, "cpu_millis": 1000, "memory_mib": 2048, "agent_limit": 1, "profile_allowlist": ["remote", "batch-standard"]})
                await response_json(client, "PATCH", f"/api/fleet/machines/{node_ids[1]}", json={"admin_state": "draining"})
                for index, node_id in enumerate(node_ids):
                    credential = await response_json(client, "POST", f"/api/fleet/machines/{node_id}/credentials", json={"lifetime_seconds": 1200})
                    process, log = await start_stock_node(
                        directory=tmp_path / ("stock-" + str(index)),
                        url=worker_url,
                        credential=credential["token"],
                        private=private,
                        image=image,
                        nas=nas,
                        evidence=evidence / ("stock-" + str(index) + ".log"),
                        kind="mixed",
                        worker_image=worker_image,
                        worker_ref=worker_refs[index],
                        worker_ca_file=ca_file,
                    )
                    processes.append(process)
                    logs.append(log)

                async def nodes_ready():
                    async with db.engine.connect() as connection:
                        return await connection.scalar(text("SELECT count(*)=2 FROM fleet_nodes WHERE session_id IS NOT NULL AND health='online'"))

                await bounded_wait(nodes_ready, seconds=60, processes=processes)
                with socket.socket() as sock:
                    sock.bind(("127.0.0.1", 0))
                    frontend_port = sock.getsockname()[1]
                frontend_url = f"http://127.0.0.1:{frontend_port}"
                monkeypatch.setenv("GATEWAY_CORS_ORIGINS", frontend_url)
                env = dict(os.environ)
                for key in ("DEER_FLOW_AUTH_DISABLED", "NEXT_PUBLIC_BACKEND_BASE_URL", "NEXT_PUBLIC_LANGGRAPH_BASE_URL"):
                    env.pop(key, None)
                env.update(DEER_FLOW_INTERNAL_GATEWAY_BASE_URL=url, DEER_FLOW_TRUSTED_ORIGINS=frontend_url, DEER_FLOW_DEV_ALLOWED_ORIGINS=frontend_url, SKIP_ENV_VALIDATION="1")
                next_log = (evidence / "next.log").open("w")
                logs.append(next_log)
                next_process = await asyncio.create_subprocess_exec(
                    sys.executable, str(ROOT / "scripts/pnpm.py"), "dev", "--hostname", "127.0.0.1", "--port", str(frontend_port), cwd=ROOT / "frontend", env=env, stdout=next_log, stderr=next_log, start_new_session=True
                )

                async def frontend_ready():
                    try:
                        async with httpx.AsyncClient(timeout=5) as probe:
                            return (await probe.get(frontend_url + "/login")).status_code == 200
                    except httpx.HTTPError:
                        return False

                await bounded_wait(frontend_ready, seconds=180, processes=[next_process, *processes])
                await response_json(client, "POST", "/api/threads", json={"thread_id": thread_id})

                async def observe_original_processes():
                    observations = {}
                    try:
                        while True:
                            async with db.engine.connect() as connection:
                                rows = (await connection.execute(text("SELECT id,kind,run_id,job_id,node_id,node_session_id,process_ref FROM fleet_attempts WHERE start_authorized_at IS NOT NULL"))).mappings().all()
                            for row in rows:
                                if row["id"] in observations or not row["process_ref"]:
                                    continue
                                observed = await driver.inspect(row["process_ref"])
                                if observed and observed["State"]["Running"] and observed["State"]["Pid"] > 0:
                                    observations[row["id"]] = {"attempt": dict(row), "container": observed}
                                    save(evidence, "running-container-observations.json", observations)
                            await asyncio.sleep(0.1)
                    finally:
                        save(evidence, "running-container-observations.json", observations)

                observer = asyncio.create_task(observe_original_processes())
                run = await response_json(
                    client,
                    "POST",
                    f"/api/threads/{thread_id}/runs",
                    json={
                        "input": {"messages": [{"role": "user", "content": "Submit bc10-original-child, await its accepted result, consume it and finish this same goal."}]},
                        "config": {"recursion_limit": 1000, "context": {"model_name": "model-1", "subagent_enabled": False}},
                        "stream_mode": ["values", "messages-tuple", "custom"],
                        "execution": {"preference": "remote", "profile": "remote"},
                    },
                )
                save(
                    evidence,
                    "admission.json",
                    {
                        "thread_id": thread_id,
                        "run": run,
                        "user_id": user["id"],
                        "schema": db.schema,
                        "redis_pid": redis_pid,
                        "gateway_url": url,
                        "frontend_url": frontend_url,
                        "stock_cli_pids": [process.pid for process in processes],
                        "next_pid": next_process.pid,
                    },
                )

                async def yielded_stopped():
                    async with db.engine.connect() as connection:
                        row = (
                            (
                                await connection.execute(
                                    text(
                                        "SELECT a.id,a.node_id,a.process_ref,a.stopped_at,res.state AS reservation,t.state AS task_state "
                                        "FROM fleet_attempts a JOIN fleet_reservations res ON res.attempt_id=a.id "
                                        "JOIN fleet_run_placements p ON p.active_attempt_id=a.id "
                                        "JOIN fleet_agent_tasks t ON t.id=p.agent_task_id WHERE a.run_id=:run"
                                    ),
                                    {"run": run["run_id"]},
                                )
                            )
                            .mappings()
                            .first()
                        )
                    if not row or row["stopped_at"] is None or row["reservation"] != "released" or row["task_state"] != "waiting_jobs":
                        return False
                    observed = await driver.inspect(row["process_ref"])
                    return {"sql": dict(row), "container": observed} if observed and not observed["State"]["Running"] else False

                stopped = await bounded_wait(yielded_stopped, seconds=150, processes=processes)
                save(evidence, "original-yield-stop.json", stopped)
                assert stopped["sql"]["node_id"] == node_ids[0]
                changes = []
                for node_id, state in ((node_ids[0], "draining"), (node_ids[1], "enabled")):
                    changes.append(await response_json(client, "PATCH", f"/api/fleet/machines/{node_id}", json={"admin_state": state}))
                save(evidence, "authorized-machine-drain.json", changes)

                async def child_started():
                    return control["child_started"].is_set()

                await bounded_wait(child_started, seconds=60, processes=processes)
                tasks = await response_json(client, "GET", f"/api/threads/{thread_id}/agent-tasks")
                task = tasks[0]
                save(evidence, "waiting-owned-summary.json", task)
                browser_env = {
                    **env,
                    "PLAYWRIGHT_BASE_URL": frontend_url,
                    "PLAYWRIGHT_SKIP_WEB_SERVER": "1",
                    "BC10_THREAD_ID": thread_id,
                    "BC10_TASK_ID": task["task_id"],
                    "BC10_EMAIL": email,
                    "BC10_PASSWORD": password,
                    "BC10_PROVIDER_URL": provider_url,
                    "BC10_BROWSER_TOKEN": control["browser_token"],
                    "BC10_EVIDENCE_DIR": str(evidence),
                }
                browser_log = (evidence / "browser.log").open("w")
                logs.append(browser_log)
                browser = await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(ROOT / "scripts/pnpm.py"),
                    "exec",
                    "playwright",
                    "test",
                    "tests/e2e/fleet-continuation.spec.ts",
                    "--config=playwright.config.ts",
                    "--workers=1",
                    "--retries=0",
                    "--reporter=json",
                    cwd=ROOT / "frontend",
                    env=browser_env,
                    stdout=browser_log,
                    stderr=browser_log,
                )
                async with asyncio.timeout(240):
                    await asyncio.shield(browser.wait())
                # Collect original facts before checking runtime/browser outcomes.
                await snapshot(db, nas, driver, worker_refs, thread_id=thread_id, calls=calls, evidence=evidence)
                assert browser.returncode == 0, "Actual authenticated same-flow browser failed; retain raw browser log"
                derived = collect_main(evidence)
                save(evidence, "main-derived.json", derived)
                assert derived["c_b_c_across_nodes"]
                assert derived["duplicate_effects"] == derived["thread_double_writes"] == derived["capacity_leaks"] == 0
            finally:
                control["child_release"].set()
                cleanup = {"worker_refs": worker_refs, "execution_refs": [], "errors": [], "remaining": [], "live_handles": []}
                try:
                    await snapshot(db, nas, driver, worker_refs, thread_id=thread_id, calls=calls, evidence=evidence)
                except Exception as error:
                    cleanup["errors"].append({"phase": "raw-capture", "error_type": type(error).__name__})
                if observer is not None:
                    observer.cancel()
                    observed_exit = await asyncio.gather(observer, return_exceptions=True)
                    for error in observed_exit:
                        if isinstance(error, BaseException) and not isinstance(error, asyncio.CancelledError):
                            cleanup["errors"].append({"phase": "original-process-observer", "error_type": type(error).__name__})
                # Capture failure cannot bypass original owned process teardown.
                for worker_ref in worker_refs:
                    try:
                        await driver.checked("stop", "--time", "130", worker_ref, timeout=140)
                    except Exception as error:
                        cleanup["errors"].append({"ref": worker_ref, "error_type": type(error).__name__})
                for process in processes:
                    try:
                        await asyncio.wait_for(process.wait(), 15)
                    except TimeoutError:
                        cleanup["live_handles"].append({"pid": process.pid, "kind": "stock-docker-cli"})
                for node_id in node_ids:
                    try:
                        refs = await driver.list_managed(node_id)
                    except Exception as error:
                        cleanup["errors"].append({"node_id": node_id, "phase": "discover", "error_type": type(error).__name__})
                        retention["live"] = True
                        refs = []
                    for ref, _ in refs:
                        cleanup["execution_refs"].append(ref)
                        try:
                            _, stdout, stderr = await driver.command("logs", ref, timeout=10)
                            output = stdout + stderr
                            for secret in (db.host_url, db.runner_url, db.password, password, private["models"][0]["api_key"]):
                                if secret:
                                    output = output.replace(secret, "[private redacted]")
                            (evidence / (ref + ".log")).write_text(output)
                        except Exception as error:
                            cleanup["errors"].append({"ref": ref, "phase": "logs", "error_type": type(error).__name__})
                        try:
                            await driver.stop(ref)
                            observed = await driver.inspect(ref)
                            if observed and observed["State"]["Running"]:
                                raise RuntimeError("Original physical stop not observed")
                            await driver.checked("rm", ref)
                        except Exception as error:
                            cleanup["errors"].append({"ref": ref, "phase": "execution-stop", "error_type": type(error).__name__})
                            retention["live"] = True
                for worker_ref in worker_refs:
                    try:
                        observed = await driver.inspect(worker_ref)
                        if observed and observed["State"]["Running"]:
                            retention["live"] = True
                            cleanup["live_handles"].append({"ref": worker_ref, "state": observed["State"]})
                        elif observed:
                            await driver.checked("rm", worker_ref)
                    except Exception as error:
                        retention["live"] = True
                        cleanup["errors"].append({"ref": worker_ref, "phase": "worker-inspect", "error_type": type(error).__name__})
                for process in (browser, next_process):
                    if process is not None and process.returncode is None:
                        try:
                            if process is next_process:
                                import signal

                                os.killpg(process.pid, signal.SIGTERM)
                            else:
                                process.terminate()
                            await asyncio.wait_for(process.wait(), 15)
                        except Exception as error:
                            cleanup["errors"].append({"pid": process.pid, "phase": "frontend-stop", "error_type": type(error).__name__})
                            cleanup["live_handles"].append({"pid": process.pid, "kind": "frontend"})
                for node_id in node_ids:
                    try:
                        cleanup["remaining"].extend(await driver.list_managed(node_id))
                    except Exception as error:
                        retention["live"] = True
                        cleanup["errors"].append({"node_id": node_id, "phase": "final-discover", "error_type": type(error).__name__})
                retention["live"] = retention["live"] or bool(cleanup["remaining"] or cleanup["live_handles"])
                cleanup["retained_schema_for_unresolved_owned_execution"] = retention["live"]
                cleanup["worker_exit_codes"] = [process.returncode for process in processes]
                cleanup["browser_exit_code"] = browser.returncode if browser else None
                cleanup["next_exit_code"] = next_process.returncode if next_process else None
                try:
                    save(evidence, "owned-cleanup.json", cleanup)
                finally:
                    for log in logs:
                        log.close()
                assert not cleanup["errors"] and not cleanup["remaining"] and not cleanup["live_handles"], "Exact owned cleanup incomplete; retain original handles and receipt"
