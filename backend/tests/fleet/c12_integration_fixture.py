"""C12 owned normal Gateway/HTTP provider fixtures; no alternate Agent graph."""

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .c04_integration_fixture import ControlDatabase, node_server


@asynccontextmanager
async def owned_database(evidence, *, retention=None):
    base = make_url(os.environ["DEERFLOW_TEST_POSTGRES_URL"])
    schema = "c12_main_" + uuid.uuid4().hex
    host = base.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
    runner = base.set(drivername="postgresql+asyncpg", host="host.docker.internal").render_as_string(hide_password=False)
    admin = create_async_engine(host)
    engine = create_async_engine(host, connect_args={"server_settings": {"search_path": schema}})
    proof = {"owned_schema": schema, "created": False, "dropped": False}
    try:
        async with admin.begin() as connection:
            await connection.execute(text('CREATE SCHEMA "' + schema + '"'))
        proof["created"] = True
        yield ControlDatabase(engine, async_sessionmaker(engine, expire_on_commit=False), schema, runner, host, base.password or "", base.port or 5432)
    finally:
        await engine.dispose()
        if retention is not None and retention.get("live"):
            proof["retained_for_unresolved_owned_execution"] = True
        elif proof["created"]:
            async with admin.begin() as connection:
                await connection.execute(text('DROP SCHEMA "' + schema + '" CASCADE'))
            proof["dropped"] = True
        await admin.dispose()
        (evidence / "owned-schema.json").write_text(json.dumps(proof, indent=2))


@asynccontextmanager
async def deterministic_provider(evidence, *, control=None):
    app = FastAPI()
    calls = []

    @app.post("/v1/chat/completions")
    async def complete(request: Request):
        body = await request.json()
        tools = [item for item in body["messages"] if item["role"] == "tool"]
        names = {item["function"]["name"] for item in body.get("tools", [])}
        assert {"bash", "present_files"} <= names
        if len(tools) > 2:
            (evidence / "unexpected-tool-return.json").write_text(json.dumps({"tool_result_count": len(tools), "last_tool_id": tools[-1].get("tool_call_id"), "last_tool_content": tools[-1]["content"]}, indent=2))
            raise RuntimeError("Held original shell returned before the revocation controller")
        revocation = "c12-revoke" in str(body["messages"])
        if control is not None and len(tools) == 2 and not revocation:
            control["delivery_waiting"].set()
            await control["delivery_release"].wait()
        call = None
        if not tools:
            call = {
                "id": "c12-bash",
                "type": "function",
                "function": {
                    "name": "bash",
                    "arguments": json.dumps({"description": "Create the deterministic artifact", "command": "printf 'c12-original-artifact\\n' > /mnt/user-data/outputs/result.txt; cat /mnt/user-data/outputs/result.txt"}),
                },
            }
        elif len(tools) == 1:
            assert tools[0]["content"].strip() == "c12-original-artifact", tools[0]["content"]
            call = {"id": "c12-present", "type": "function", "function": {"name": "present_files", "arguments": json.dumps({"filepaths": ["/mnt/user-data/outputs/result.txt"]})}}
        else:
            assert tools[-1]["content"] == "Successfully presented files", tools[-1]["content"]
            if revocation:
                call = {
                    "id": "c12-held-shell",
                    "type": "function",
                    "function": {
                        "name": "bash",
                        "arguments": json.dumps(
                            {
                                "description": "Hold the original supervised shell for revocation",
                                "command": "printf 'original-shell-started\\n' > /mnt/user-data/workspace/c12-revoke-shell-start; sleep 90",
                            }
                        ),
                    },
                }

        calls.append({"model": body["model"], "message_roles": [item["role"] for item in body["messages"]], "tool_result_count": len(tools), "chosen_tool": call["function"]["name"] if call else None})
        (evidence / "provider-observations.json").write_text(json.dumps(calls, indent=2))
        message = {"role": "assistant", "content": "" if call else "Completed the deterministic artifact."}
        if call:
            message["tool_calls"] = [call]
        common = {"id": "chatcmpl-c12-" + str(len(calls)), "created": 1791216000, "model": body["model"]}
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
        yield url, calls


def private_configuration(db, provider_url):
    return {
        "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider", "allow_host_bash": True},
        "database": {"backend": "postgres", "postgres_url": db.runner_url, "postgres_schema": db.schema, "checkpoint_channel_mode": "full"},
        "run_events": {"backend": "db"},
        "run_ownership": {"heartbeat_enabled": True},
        "agent_storage": {"backend": "db"},
        "plugins": [],
        "memory": {"enabled": False, "manager_class": "deerflow.agents.memory.backends.noop:NoopMemoryManager"},
        "mcp_tasks": {"enabled": True},
        "title": {"enabled": False},
        "summarization": {"enabled": False},
        "models": [
            {"name": "model-1", "use": "langchain_openai:ChatOpenAI", "model": "c12-deterministic", "base_url": provider_url.replace("127.0.0.1", "host.docker.internal") + "/v1", "api_key": "c12-owned-provider-token", "max_retries": 0}
        ],
        "tools": [{"name": "bash", "group": "bash", "use": "deerflow.sandbox.tools:bash_tool"}],
        "tool_groups": [{"name": "bash", "description": "Create a deterministic artifact"}],
        "extensions": {"skills": {}, "mcpServers": {}},
    }


@asynccontextmanager
async def normal_gateway(payload):
    from app.gateway.app import create_app
    from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config

    set_app_config(AppConfig.model_validate(payload))
    app = create_app()
    try:
        async with app.router.lifespan_context(app):
            async with node_server(app) as url:
                async with httpx.AsyncClient(base_url=url, timeout=180) as client:
                    yield app, client, url
    finally:
        reset_app_config()


async def response_json(client, method, path, **kwargs):
    response = await client.request(method, path, **kwargs)
    assert response.status_code < 300, (path, response.status_code, response.text)
    return response.json()


async def login(client, email, password):
    await response_json(client, "POST", "/api/v1/auth/login/local", data={"username": email, "password": password})
    client.headers["X-CSRF-Token"] = client.cookies["csrf_token"]


async def durable_state(db, run):
    async with db.session_factory() as session:
        return dict(
            (
                await session.execute(
                    text(
                        "SELECT r.status,t.state AS task_state,p.state AS placement_state,a.stopped_at,res.state AS reservation,"
                        "t.accepted_workspace_point_id,p.final_workspace_point_id FROM runs r JOIN fleet_run_placements p ON p.run_id=r.run_id "
                        "JOIN fleet_agent_tasks t ON t.id=p.agent_task_id JOIN fleet_attempts a ON a.id=p.active_attempt_id "
                        "JOIN fleet_reservations res ON res.attempt_id=a.id WHERE r.run_id=:run"
                    ),
                    {"run": run},
                )
            )
            .mappings()
            .one()
        )


async def start_stock_node(*, directory, url, credential, private, image, nas, evidence, kind="agent", worker_image=None, worker_ref=None, worker_ca_file=None):
    """Launch the unmodified stock Node CLI with private local settings/journal."""
    import sys

    directory.mkdir(mode=0o700 if worker_image is not None else 0o777, parents=True, exist_ok=True)
    operator = directory / "operator.json"
    token = directory / "credential"
    settings = directory / "settings.json"
    operator.write_text(json.dumps(private))
    token.write_text(credential)
    settings.write_text(
        json.dumps(
            {
                "kind": kind,
                "agent_image": image,
                "agent_config_file": str(operator),
                "gateway_url": url,
                "credential_file": str(token),
                "state_dir": str(directory / "journal"),
                "nas_root": str(nas),
                "nas_identity": "fleet-test",
                "renew_seconds": 1,
                "safety_margin_seconds": 0.25,
                "poll_seconds": 0.05,
            }
        )
    )
    if worker_image is not None:
        if not worker_ref:
            raise ValueError("Installed worker requires an explicit owned container identity")
        payload = json.loads(settings.read_text())
        payload.update(gateway_url=url.replace("127.0.0.1", "host.docker.internal"), renew_seconds=10, safety_margin_seconds=5, poll_seconds=0.25)
        settings.write_text(json.dumps(payload))
    for path in (operator, token, settings):
        path.chmod(0o600)
    env = dict(os.environ)
    env["PATH"] = str(__import__("pathlib").Path(os.environ["C12_DOCKER"]).parent) + os.pathsep + env["PATH"]
    env["PYTHONPATH"] = os.pathsep.join(str(__import__("pathlib").Path(item).resolve()) for item in sys.path if item)
    log = evidence.open("w")
    argv = [sys.executable, "-m", "deerflow_ecs_fleet.worker", "--settings", str(settings)]
    if worker_image is not None:
        argv = [
            os.environ["C12_DOCKER"],
            "run",
            "--pull=never",
            "--name",
            worker_ref,
            "--restart=no",
            "--user",
            str(os.getuid()) + ":" + str(os.getgid()),
            "--group-add",
            str(0 if sys.platform == "darwin" else __import__("pathlib").Path(os.environ["FLEET_TEST_DOCKER_SOCKET"]).stat().st_gid),
            "--env",
            "HOME=/tmp",
            "--mount",
            "type=bind,src=" + os.environ["FLEET_TEST_DOCKER_SOCKET"] + ",dst=/var/run/docker.sock",
            "--mount",
            "type=bind,src=" + str(directory) + ",dst=" + str(directory),
            "--mount",
            "type=bind,src=" + str(nas) + ",dst=" + str(nas),
            worker_image,
            "--settings",
            str(settings),
        ]
    if worker_image is not None and worker_ca_file is not None:
        ca_file = directory / "gateway-ca.pem"
        ca_file.write_bytes(worker_ca_file.read_bytes())
        ca_file.chmod(0o600)
        argv[2:2] = ["--env", "SSL_CERT_FILE=" + str(ca_file)]
    process = await asyncio.create_subprocess_exec(*argv, env=env, stdout=log, stderr=log)
    return process, log


async def table_fingerprints(db):
    """Hash real rows of mutation domains, excluding Node liveness/control rows."""
    import hashlib

    tables = ("checkpoints", "checkpoint_blobs", "checkpoint_writes", "store", "run_events", "fleet_event_outbox", "fleet_workspace_points", "fleet_workspace_manifests")
    async with db.session_factory() as session:
        result = {}
        for table in tables:
            projection = "(to_jsonb(t)-'published_at')::text" if table == "fleet_event_outbox" else "row_to_json(t)::text"
            rows = (await session.execute(text("SELECT " + projection + " FROM " + table + " t ORDER BY " + projection))).scalars().all()
            result[table] = {"rows": len(rows), "sha256": hashlib.sha256(json.dumps(rows).encode()).hexdigest()}
        return result


async def installed_late_checkpoint_probe(*, docker, image, private, journal, checkpoint):
    """Independent installed-saver probe; does not impersonate original runner work."""
    import hashlib

    grant, claim = journal["grant"], journal["claim"]
    identity = {key: grant[key] for key in ("node_id", "node_session_id", "agent_task_id", "generation", "attempt_id", "owner_worker_id")}
    identity["token_stamp"] = hashlib.sha256(claim["token"].encode()).hexdigest()
    import zipfile
    from pathlib import Path

    artifacts = Path(__file__).resolve().parents[3] / ".local/fleet-evidence/c12/production-artifacts"
    expected = {}
    for wheel in (artifacts / "wheelhouse").glob("*.whl"):
        if wheel.name.startswith("websockets-"):
            continue
        with zipfile.ZipFile(wheel) as archive:
            metadata = next(name for name in archive.namelist() if name.endswith(".dist-info/METADATA"))
            name = next(line.removeprefix("Name: ") for line in archive.read(metadata).decode().splitlines() if line.startswith("Name: "))
            expected[name] = {member: hashlib.sha256(archive.read(member)).hexdigest() for member in archive.namelist() if member.endswith(".py")}
    frozen = json.loads((artifacts / "frozen-inputs.json").read_text())
    payload = {
        "private": private,
        "identity": identity,
        "spec": grant["launch_spec"],
        "checkpoint": checkpoint,
        "expected": expected,
        "assets": {name: frozen[name] for name in ("model-bindings.json", "runtime-bundle.json", "workspace-contracts.json")},
    }
    code = """import asyncio,json,sys,hashlib
from pathlib import Path
from importlib.metadata import distribution
from deerflow.config.app_config import AppConfig
from deerflow_ecs_fleet.worker.agent_environment import ExecutionIdentity
from deerflow_ecs_fleet.launch_spec import LaunchSpec
from app.fleet.mutation import FleetCheckpointFence,FleetMutationCapability
from deerflow.runtime.checkpointer.async_provider import make_checkpointer
from deerflow.runtime.execution.mutation_context import remote_mutation_scope,OwnershipRejected
async def main():
 d=json.load(sys.stdin)
 audit={"distributions":{},"copied_files":{},"approved_assets":{},"scope":"actual installed image bytes against frozen production wheels/assets"}
 for name,members in d["expected"].items():
  dist=distribution(name); actual={member:hashlib.sha256(Path(dist.locate_file(member)).read_bytes()).hexdigest() for member in members}
  assert actual==members, "Installed production source differs from frozen wheel"
  audit["distributions"][name]={"version":dist.version,"members":actual}
 from deerflow_ecs_fleet.worker import libexec_bootstrap,workspace_collector
 for path,source in (("/opt/deerflow/libexec_bootstrap.py",libexec_bootstrap.__file__),(workspace_collector.COLLECTOR_PATH,workspace_collector.__file__)):
  digest=hashlib.sha256(Path(path).read_bytes()).hexdigest(); assert digest==hashlib.sha256(Path(source).read_bytes()).hexdigest();audit["copied_files"][str(path)]=digest
 for name,expected in d["assets"].items():
  digest=hashlib.sha256(Path("/opt/deerflow",name).read_bytes()).hexdigest();assert digest==expected;audit["approved_assets"][name]=digest
 identity=ExecutionIdentity(**d["identity"]); spec=LaunchSpec.model_validate(d["spec"]); capability=FleetMutationCapability(identity,spec)
 with remote_mutation_scope(capability.context):
  async with make_checkpointer(AppConfig.model_validate(d["private"]),write_fence=FleetCheckpointFence(identity,spec)) as saver:
   try: await saver.aput_writes(d["checkpoint"],[("messages","c12-stale-checkpoint-write")],"c12-stale-task")
   except OwnershipRejected as error:
    print(json.dumps({"rejected":True,"exception":type(error).__name__,"operation":"actual installed saver.aput_writes",
     "scope":"independent original-identity probe, not original runner write","installed_audit":audit}));return
   raise RuntimeError("Revoked execution wrote checkpoint")
asyncio.run(main())"""
    process = await asyncio.create_subprocess_exec(
        docker,
        "run",
        "--rm",
        "-i",
        "--pull=never",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--entrypoint",
        "python",
        image,
        "-c",
        code,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate(json.dumps(payload).encode())
    assert process.returncode == 0, stderr.decode()[-1000:]
    return json.loads(stdout)
