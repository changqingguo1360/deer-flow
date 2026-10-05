"""Owned native C07 baseline: real host environment, AgentRunner, PG, Redis and HTTP."""

import asyncio
import json
import os
import socket
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import patch

import httpx
from langchain.tools import ToolRuntime, tool
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from redis.asyncio import Redis
from sqlalchemy import text

_RUNNER_DIR = None


def receipt(event, **fields):
    global _RUNNER_DIR
    if _RUNNER_DIR is None:
        # Installed libexec derives its owned mounted workspace from the same
        # trusted environment used by AgentContainers; native child sets it.
        _RUNNER_DIR = Path(os.environ["DEER_FLOW_HOME"]).parent
    assert _RUNNER_DIR.is_dir()
    with (_RUNNER_DIR / "process.jsonl").open("a") as out:
        out.write(json.dumps({"event": event, "pid": os.getpid(), **fields}) + "\n")
        out.flush()


@tool
async def c07_barrier(runtime: ToolRuntime) -> str:
    """Wait for the acceptance controller before returning the committed tool tail."""
    journal = runtime.context["__run_journal"]
    await journal.flush()
    receipt("barrier-entered")
    deadline = asyncio.get_running_loop().time() + 30
    while not (_RUNNER_DIR / "release").exists():
        if asyncio.get_running_loop().time() > deadline:
            raise RuntimeError("C07 controller did not release owned barrier")
        await asyncio.sleep(0.02)
    receipt("barrier-released")
    return "c07-tail-tool"


class BarrierModel(BaseChatModel):
    model: str = "c07"

    @property
    def _llm_type(self):
        return "c07-real-scripted-provider"

    def bind_tools(self, tools, **kwargs):
        return self

    def answer(self, messages):
        if not any(isinstance(m, ToolMessage) for m in messages):
            return AIMessage(content="", tool_calls=[{"id": "c07-barrier-call", "type": "tool_call", "name": "c07_barrier", "args": {}}], usage_metadata={"input_tokens": 4, "output_tokens": 2, "total_tokens": 6})
        return AIMessage(content="c07-final-tail", usage_metadata={"input_tokens": 5, "output_tokens": 3, "total_tokens": 8})

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        return ChatResult(generations=[ChatGeneration(message=self.answer(messages))])

    def _stream(self, messages, stop=None, run_manager=None, **kwargs):
        message = self.answer(messages)
        yield ChatGenerationChunk(message=AIMessageChunk(**message.model_dump(exclude={"type"})))


@asynccontextmanager
async def native_runtime_paths(directory):
    """Relocate immutable /opt operator fixture files, preserving real validation."""
    import app.fleet.runner_context as host

    original = Path

    def native_path(value, *args):
        value = str(value)
        if value == "/opt/deerflow" or value.startswith("/opt/deerflow/"):
            return original(directory, value.removeprefix("/opt/deerflow").lstrip("/"), *args)
        return original(value, *args)

    with patch.object(host, "Path", native_path):
        yield host


async def child_main():
    global _RUNNER_DIR
    from deerflow_ecs_fleet.launch_spec import LaunchSpec
    from deerflow_ecs_fleet.worker.agent_environment import BootstrapV1
    from deerflow_ecs_fleet.worker.agent_runner import AgentRunner

    data = json.loads(sys.stdin.buffer.readline())
    _RUNNER_DIR = Path(data["directory"])
    async with native_runtime_paths(_RUNNER_DIR / "runtime") as host:
        if data.get("compatibility"):
            print(host.installed_compatibility().model_dump_json())
            return
        import hashlib

        from deerflow_ecs_fleet.persistence import outbox as actual_outbox

        origins = {}
        import deerflow_ecs_fleet.persistence.models as actual_models

        import app.fleet.events as actual_events
        import deerflow.runtime.events.store.db as actual_store
        import deerflow.runtime.events.transactions as actual_transactions

        for module in (host, actual_outbox, actual_events, actual_store, actual_transactions, actual_models):
            source = Path(module.__file__).resolve()
            origins[module.__name__] = {"file": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
        receipt("source-origins", modules=origins)
        receipt("runner-start")
        # Stock factory lacks a config key-prefix option; retain stock constructor
        # and only inject its existing trusted key_prefix keyword for owned keys.
        from deerflow.runtime.stream_bridge.redis import RedisStreamBridge

        original_redis_init = RedisStreamBridge.__init__

        def owned_redis_init(instance, **kwargs):
            kwargs["key_prefix"] = (_RUNNER_DIR / "redis-prefix.txt").read_text()
            original_redis_init(instance, **kwargs)

        prefix_patch = patch.object(RedisStreamBridge, "__init__", owned_redis_init)
        prefix_patch.start()
        environment = await host.build_agent_environment(
            bootstrap=BootstrapV1.from_private_payload(data["bootstrap"]),
            spec=LaunchSpec.model_validate(data["spec"]),
            grant=data["grant"],
        )
        try:
            record = await AgentRunner().run(
                LaunchSpec.model_validate(data["spec"]),
                grant=data["grant"],
                environment=environment,
            )
            receipt("runner-return", status=record.status.value, ownership_lost=record.ownership_lost)
        except Exception as exc:
            # Original baseline Redis publication error must not mask terminal DB evidence.
            receipt("runner-exception", error_type=type(exc).__name__, writer_seal_fault="actual C07 writer seal fault" in str(exc))
        finally:
            await environment.close()
            receipt("runner-exit")
            prefix_patch.stop()


class RedisFaultProxy:
    """Actual TCP connection cutoff and accepted-write/lost-reply fault control."""

    def __init__(self, port):
        self.upstream_port = port
        self.available = True
        self.connections = set()
        self.tasks = set()
        self.lost_response = False
        self.bytes_to_redis = 0
        self.bytes_from_redis = 0
        self.dropped_replies = 0
        self.command_receipts = []
        self.hold_delivery_reply = False
        self.delivery_reply_held = asyncio.Event()
        self.release_delivery_reply = asyncio.Event()

    async def start(self, *, host="127.0.0.1"):
        self.server = await asyncio.start_server(self.accept, host, 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def accept(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        self.connections.add(writer)
        upstream = None
        try:
            if not self.available:
                return
            remote_reader, upstream = await asyncio.open_connection("127.0.0.1", self.upstream_port)
            self.connections.add(upstream)

            while True:
                command_bytes, command = await self.read_resp(reader)
                self.bytes_to_redis += len(command_bytes)
                upstream.write(command_bytes)
                await upstream.drain()
                reply_bytes, reply = await self.read_resp(remote_reader)
                self.bytes_from_redis += len(reply_bytes)
                name = command[0].decode().upper() if isinstance(command, list) and command else ""
                if name in {"EVAL", "EVALSHA", "XADD"}:
                    self.command_receipts.append({"command": name, "response_type": reply_bytes[:1].decode(), "response": reply, "accepted": reply_bytes[:1] != b"-"})
                if self.hold_delivery_reply and name in {"EVAL", "EVALSHA", "XADD"} and reply_bytes[:1] != b"-":
                    self.delivery_reply_held.set()
                    await self.release_delivery_reply.wait()
                if self.lost_response and name in {"EVAL", "EVALSHA", "XADD"} and reply_bytes[:1] != b"-":
                    self.lost_response = False
                    self.dropped_replies += 1
                    self.command_receipts[-1]["reply_dropped"] = True
                    return
                writer.write(reply_bytes)
                await writer.drain()
        except (ConnectionError, OSError, EOFError, asyncio.IncompleteReadError):
            pass
        finally:
            for stream in (writer, upstream):
                if stream is not None:
                    stream.close()
                    self.connections.discard(stream)
            self.tasks.discard(task)

    @classmethod
    async def read_resp(cls, reader):
        line = await reader.readline()
        if not line:
            raise EOFError("RESP connection closed")
        prefix, value = line[:1], line[1:-2]
        if prefix == b"$":
            length = int(value)
            if length < 0:
                return line, None
            body = await reader.readexactly(length + 2)
            return line + body, body[:-2]
        if prefix == b"*":
            count = int(value)
            if count < 0:
                return line, None
            raw, values = line, []
            for _ in range(count):
                item, parsed = await cls.read_resp(reader)
                raw += item
                values.append(parsed)
            return raw, values
        if prefix == b":":
            return line, int(value)
        if prefix in {b"+", b"-"}:
            return line, value.decode()
        raise ValueError(f"Unsupported RESP2 prefix {prefix!r}")

    async def disconnect(self):
        self.available = False
        for writer in tuple(self.connections):
            writer.close()
        if self.tasks:
            tasks = tuple(self.tasks)
            for task in tasks:
                task.cancel()
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 3)

    async def close(self):
        await self.disconnect()
        self.server.close()
        await self.server.wait_closed()


@asynccontextmanager
async def owned_redis(directory):
    directory.mkdir()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    log = (directory / "redis.log").open("wb")
    process = await asyncio.create_subprocess_exec(
        "/opt/homebrew/bin/redis-server",
        "--bind",
        "127.0.0.1",
        "--port",
        str(port),
        "--dir",
        str(directory),
        "--save",
        "",
        "--appendonly",
        "no",
        stdout=log,
        stderr=asyncio.subprocess.STDOUT,
    )
    client = Redis(host="127.0.0.1", port=port, socket_connect_timeout=1, socket_timeout=1)
    try:
        async with asyncio.timeout(5):
            while True:
                try:
                    assert await client.ping()
                    break
                except ConnectionError:
                    await asyncio.sleep(0.05)
                except Exception:
                    if process.returncode is not None:
                        raise RuntimeError("Owned Redis startup failed") from None
                    await asyncio.sleep(0.05)
        yield client, port, process.pid
    finally:
        await client.aclose()
        if process.returncode is None:
            process.terminate()
        await asyncio.wait_for(process.wait(), 5)
        log.close()


async def bounded_wait(predicate, *, seconds=15):
    async with asyncio.timeout(seconds):
        while not await predicate():
            await asyncio.sleep(0.02)


class C07Scenario:
    expected_tail = "c07-tail-tool"

    def __init__(self, *, directory, engine, record, claim, grant, bootstrap, app, url, node, proxy, redis, prefix):
        self.directory, self.engine, self.record = directory, engine, record
        self.claim, self.grant, self.bootstrap = claim, grant, bootstrap
        self.app, self.url, self.node = app, url, node
        self.proxy, self.redis, self.prefix = proxy, redis, prefix
        self.process = None
        self.frames = []
        self.http_receipts = []

    async def start_once(self):
        assert self.process is None
        self.log = (self.directory / "runner.log").open("wb")
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "fleet.c07_integration_fixture",
            stdin=asyncio.subprocess.PIPE,
            stdout=self.log,
            stderr=asyncio.subprocess.STDOUT,
            env={**os.environ, "DEER_FLOW_HOME": str(self.directory / "home"), "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "packages" / "ecs-fleet") + os.pathsep + os.environ.get("PYTHONPATH", "")},
        )
        packet = {"directory": str(self.directory), "bootstrap": self.bootstrap, "spec": self.claim["launch_spec"], "grant": self.grant}
        self.process.stdin.write(json.dumps(packet).encode() + b"\n")
        await self.process.stdin.drain()
        self.process.stdin.close()

    async def start_receipts(self):
        path = self.directory / "process.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
        return [row for row in rows if row["event"] == "runner-start"]

    async def wait_for_committed_model_event(self):
        async def committed():
            path = self.directory / "process.jsonl"
            if self.process.returncode is not None:
                raise RuntimeError("Runner exited before actual model barrier; see runner.log")
            if not path.exists() or '"barrier-entered"' not in path.read_text():
                return False
            async with self.engine.connect() as conn:
                rows = (await conn.execute(text("SELECT event_type,content,seq FROM run_events WHERE run_id=:id ORDER BY seq"), {"id": self.record.run_id})).all()
            self.before_rows = [list(row) for row in rows]
            return any(row[0] == "llm.ai.response" for row in rows)

        await bounded_wait(committed, seconds=30)

    async def join(self, cursor=None):
        from app.gateway.internal_auth import create_internal_auth_headers

        headers = create_internal_auth_headers(owner_user_id=self.record.user_id)
        if cursor:
            headers["Last-Event-ID"] = cursor
        frames = []
        async with httpx.AsyncClient(timeout=5) as client:
            async with client.stream("GET", f"{self.url}/api/threads/{self.record.thread_id}/runs/{self.record.run_id}/join", headers=headers) as reply:
                self.http_receipts.append({"status": reply.status_code, "content_type": reply.headers.get("content-type"), "cursor": cursor})
                reply.raise_for_status()
                current = {}
                heartbeats = 0
                try:
                    # Only the successful streaming observation has a bounded
                    # period. Startup/auth/HTTP/network errors remain failures.
                    async with asyncio.timeout(3):
                        async for line in reply.aiter_lines():
                            if line.startswith(": heartbeat"):
                                heartbeats += 1
                            elif not line:
                                if "event" in current:
                                    frames.append(current)
                                    if current["event"] == "end":
                                        break
                                current = {}
                            elif line.startswith("id: "):
                                current["id"] = line[4:]
                            elif line.startswith("event: "):
                                current["event"] = line[7:]
                            elif line.startswith("data: "):
                                current["data"] = json.loads(line[6:])
                except TimeoutError:
                    self.http_receipts.append({"phase": "bounded-reconnect-observation", "timed_out": True, "seconds": 3, "heartbeats": heartbeats, "received_frames": len(frames)})
        self.frames = frames
        return frames

    async def initial_cursor(self):
        from app.gateway.internal_auth import create_internal_auth_headers

        headers = create_internal_auth_headers(owner_user_id=self.record.user_id)
        async with httpx.AsyncClient(timeout=5) as client:
            async with asyncio.timeout(8):
                async with client.stream("GET", f"{self.url}/api/threads/{self.record.thread_id}/runs/{self.record.run_id}/join", headers=headers) as reply:
                    reply.raise_for_status()
                    self.http_receipts.append({"phase": "initial-observer", "status": reply.status_code})
                    current = {}
                    async for line in reply.aiter_lines():
                        if line.startswith("id: "):
                            current["id"] = line[4:]
                        elif line.startswith("event: "):
                            current["event"] = line[7:]
                        elif line.startswith("data: "):
                            current["data"] = json.loads(line[6:])
                        elif not line and current.get("id"):
                            self.http_receipts.append({"phase": "initial-frame", **current})
                            return current["id"]
        raise AssertionError("Actual HTTP observer received no SSE cursor")

    async def disconnect_redis(self):
        await self.proxy.disconnect()

    async def restore_redis(self):
        self.proxy.available = True

    async def release_model_barrier(self):
        (self.directory / "release").touch()

    async def wait_for_runner_exit(self):
        await asyncio.wait_for(self.process.wait(), 30)
        self.log.close()
        assert self.process.returncode == 0, "Native runner fixture must finish original resource cleanup"

    async def acknowledge_actual_stop(self):
        assert self.process.returncode is not None
        return await self.node.attempt(self.claim, "stopped", reason="exit", exit_code=self.process.returncode, process_ref=self.grant["process_ref"], physical_stopped=True)

    async def delete_owned_redis_keys(self):
        keys = [key async for key in self.redis.scan_iter(match=self.prefix + ":*")]
        if keys:
            await self.redis.delete(*keys)
        assert not [key async for key in self.redis.scan_iter(match=self.prefix + ":*")]

    async def durable_receipt(self):
        async with self.engine.connect() as conn:
            run = (await conn.execute(text("SELECT status,error,owner_worker_id FROM runs WHERE run_id=:id"), {"id": self.record.run_id})).one()
            events = (await conn.execute(text("SELECT event_type,content,seq FROM run_events WHERE run_id=:id ORDER BY seq"), {"id": self.record.run_id})).all()
            stopped = (await conn.execute(text("SELECT state,stopped_at,outcome FROM fleet_attempts WHERE id=:id"), {"id": self.claim["attempt_id"]})).one()
        return {
            "run": list(run),
            "events": [list(row) for row in events],
            "attempt": [str(stopped[0]), str(stopped[1]), stopped[2]],
            "process_pid": self.process.pid,
            "process_exit_code": self.process.returncode,
            "starts": await self.start_receipts(),
            "http": self.http_receipts,
            "frames": self.frames,
            "proxy_bytes_to_redis": self.proxy.bytes_to_redis,
        }

    async def close(self):
        if self.process is not None and self.process.returncode is None:
            await self.release_model_barrier()
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except TimeoutError:
                self.process.terminate()
                await asyncio.wait_for(self.process.wait(), 5)
        if hasattr(self, "log") and not self.log.closed:
            self.log.close()


if __name__ == "__main__":
    # Provider/tool import must resolve the same real native fixture module.
    sys.modules["fleet.c07_integration_fixture"] = sys.modules["__main__"]
    asyncio.run(child_main())


async def installed_omitting_seal_environment(*, bootstrap, spec, grant):
    """Trusted fault fixture: omit seal after real worker closure, then naturally close."""
    from app.fleet.runner_context import build_agent_environment
    from deerflow.runtime.runs.manager import RunStatus
    from fleet.c08_installed_bytes import verify_installed

    verify_installed()
    environment = await build_agent_environment(bootstrap=bootstrap, spec=spec, grant=grant)
    install_stock_final_probe(environment)
    original_close = environment.close
    original_record = None

    async def omit_end(run_id):
        nonlocal original_record
        record = await environment.manager.get(run_id)
        assert record is await environment.bridge._record(run_id)
        assert record.run_id == spec.run_id and record.thread_id == spec.thread_id and record.user_id == spec.user_id
        assert record.owner_worker_id == bootstrap.identity.owner_worker_id
        assert record.status in {RunStatus.success, RunStatus.error, RunStatus.interrupted}
        assert not record.finalizing and not record.ownership_lost
        original_record = record
        receipt("safe-seal-omitted", finalizing=record.finalizing, ownership_lost=record.ownership_lost, status=record.status.value)

    async def close():
        await original_close()
        assert original_record is not None
        assert await environment.manager.get(spec.run_id) is original_record
        assert not original_record.finalizing and not original_record.ownership_lost
        receipt("original-environment-settled")

    environment.bridge.publish_end = omit_end
    environment.close = close
    return environment


def installed_omitting_seal_compatibility():
    from app.fleet.runner_context import installed_compatibility

    return installed_compatibility()


installed_omitting_seal_environment.worker_compatibility = installed_omitting_seal_compatibility


async def installed_c07_environment(*, bootstrap, spec, grant):
    """Original C07 model/tool over actual C08 installed publication resources."""
    from app.fleet.runner_context import build_agent_environment
    from deerflow.runtime.stream_bridge.redis import RedisStreamBridge
    from fleet.c08_installed_bytes import verify_installed

    verify_installed()
    receipt("runner-start", execution_surface="installed Linux container")
    original_init = RedisStreamBridge.__init__

    def owned_init(instance, **kwargs):
        kwargs["key_prefix"] = (_RUNNER_DIR / "redis-prefix.txt").read_text()
        original_init(instance, **kwargs)

    prefix_patch = patch.object(RedisStreamBridge, "__init__", owned_init)
    prefix_patch.start()
    try:
        environment = await build_agent_environment(bootstrap=bootstrap, spec=spec, grant=grant)
    except BaseException:
        prefix_patch.stop()
        raise
    original_end = environment.bridge.publish_end
    original_close = environment.close

    async def publish_end(run_id):
        try:
            await original_end(run_id)
        except Exception as error:
            receipt("runner-exception", error_type=type(error).__name__, writer_seal_fault="actual C07 writer seal fault" in str(error))
            raise

    async def close():
        try:
            await original_close()
            receipt("runner-exit", execution_surface="installed Linux container")
        finally:
            prefix_patch.stop()

    environment.bridge.publish_end = publish_end
    environment.close = close
    return environment


installed_c07_environment.worker_compatibility = installed_omitting_seal_compatibility


def install_stock_final_probe(environment):
    """Keep the accepted closed-Service fixture check after original owner joins."""
    from dataclasses import replace

    from deerflow_c04_fixture import Service

    from fleet.c08_stock_linux_fixture import install_final_closed_service_probe

    restore = install_final_closed_service_probe(environment.workspace_publications, Service)
    original_prepare = environment.context.prepare_terminal
    original_close = environment.close

    async def prepare(record):
        restore.capture_owners()
        await original_prepare(record)

    async def close():
        try:
            await original_close()
        finally:
            restore()

    environment.context = replace(environment.context, prepare_terminal=prepare)
    environment.close = close


async def installed_c07_stock_environment(*, bootstrap, spec, grant):
    from app.fleet.runner_context import build_agent_environment
    from fleet.c08_installed_bytes import verify_installed

    verify_installed()
    environment = await build_agent_environment(bootstrap=bootstrap, spec=spec, grant=grant)
    install_stock_final_probe(environment)
    return environment


installed_c07_stock_environment.worker_compatibility = installed_omitting_seal_compatibility
