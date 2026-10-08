"""C08 original-runner fixture: controller pauses a real provider after presentation."""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from langchain_core.messages import AIMessage, ToolMessage
from pydantic import SecretStr
from sqlalchemy import text

from . import c07_integration_fixture as original

PARTIAL = b"c08-partial-v1\n"


class StageModel(original.BarrierModel):
    """Only model output is scripted; stock factory, graph and tools execute."""

    api_key: SecretStr | None = None

    def answer(self, messages):
        from deerflow.config.app_config import get_app_config
        from deerflow.config.extensions_config import get_scoped_extensions_config
        from deerflow.persistence.agent_definition_context import get_scoped_definition_stores

        assert self.api_key is not None
        assert self.api_key.get_secret_value() not in get_app_config().model_dump_json()
        assert get_scoped_extensions_config() is not None
        assert get_scoped_definition_stores() is not None
        tools = [m for m in messages if isinstance(m, ToolMessage)]
        count = len(tools)
        original.receipt("model-entered", completed_tools=count)
        if count == 0:
            import hashlib
            import importlib

            origins = {}
            for name in (
                "deerflow_ecs_fleet.worker.agent_runner",
                "deerflow.agents.lead_agent.agent",
                "deerflow.models.factory",
                "deerflow.runtime.runs.worker",
                "deerflow.runtime.checkpointer.fenced_saver",
                "deerflow.sandbox.tools",
                "deerflow.tools.builtins.present_file_tool",
            ):
                module = importlib.import_module(name)
                source = Path(module.__file__).resolve()
                origins[name] = {"file": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
            original.receipt("original-execution-origins", modules=origins)
            name, args = "write_file", {"description": "Create stage output", "path": "/mnt/user-data/outputs/partial.txt", "content": PARTIAL.decode()}
        elif count == 1:
            assert "Error" not in str(tools[-1].content), tools[-1].content
            name, args = "present_files", {"filepaths": ["/mnt/user-data/outputs/partial.txt"]}
        elif count == 2:
            assert tools[-1].content == "Successfully presented files", tools[-1].content
            original.receipt("presentation-completed", tool_call_id=tools[-1].tool_call_id)
            deadline = time.monotonic() + 45
            while not (original._RUNNER_DIR / "release").exists():
                if time.monotonic() > deadline:
                    raise RuntimeError("C08 owned controller did not release provider")
                time.sleep(0.02)
            name, args = "bash", {"description": "Update outputs after stage", "command": "printf 'c08-partial-v2\\n' > /mnt/user-data/outputs/partial.txt; printf 'c08-final\\n' > /mnt/user-data/outputs/final.txt"}
        else:
            return AIMessage(content="c08-final-tail")
        return AIMessage(content="", tool_calls=[{"id": f"c08-original-tool-{count}", "type": "tool_call", "name": name, "args": args}])


class C08Scenario(original.C07Scenario):
    async def start_once(self):
        assert self.process is None
        self.log = (self.directory / "runner.log").open("wb")
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "fleet.c08_integration_fixture",
            stdin=asyncio.subprocess.PIPE,
            stdout=self.log,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
            env={**os.environ, "DEER_FLOW_HOME": str(self.directory / "home"), "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "packages" / "ecs-fleet") + os.pathsep + os.environ.get("PYTHONPATH", "")},
        )
        packet = {"directory": str(self.directory), "bootstrap": self.bootstrap, "spec": self.claim["launch_spec"], "grant": self.grant}
        self.process.stdin.write(json.dumps(packet).encode() + b"\n")
        await self.process.stdin.drain()
        self.process.stdin.close()

    async def wait_for_stage(self):
        async def committed():
            if self.process.returncode is not None:
                raise RuntimeError("Original runner exited before presentation; see runner.log")
            path = self.directory / "process.jsonl"
            if not path.exists() or '"presentation-completed"' not in path.read_text():
                return False
            checkpoint = await self.app.state.checkpointer.aget_tuple({"configurable": {"thread_id": self.record.thread_id}})
            if checkpoint is None:
                return False
            messages = checkpoint.checkpoint["channel_values"].get("messages", [])
            presented = [m for m in messages if isinstance(m, ToolMessage) and m.tool_call_id == "c08-original-tool-1" and m.content == "Successfully presented files"]
            if not presented:
                return False
            self.stage_checkpoint = checkpoint.config["configurable"]["checkpoint_id"]
            from deerflow.config.paths import Paths

            self.remote_file = Paths(base_dir=self.directory / "home").sandbox_outputs_dir(self.record.thread_id, user_id=self.record.user_id) / "partial.txt"
            assert self.remote_file.read_bytes() == PARTIAL
            assert not self.remote_file.with_name("final.txt").exists()
            return True

        await original.bounded_wait(committed, seconds=30)

    async def acknowledge_actual_stop(self):
        assert self.process.returncode is not None
        raw = self.process.returncode
        wire = 128 - raw if raw < 0 else raw
        result = await self.node.attempt(self.claim, "stopped", reason="exit", exit_code=wire, process_ref=self.grant["process_ref"], physical_stopped=True)
        self.http_receipts.append({"operation": "stopped", "raw_native_returncode": raw, "wire_exit_code": wire, "response": result})
        return result

    def tool_receipts(self):
        path = self.directory / "process.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines() if json.loads(line)["event"] in {"tool-start", "tool-return"}]

    async def sql_state(self):
        async with self.engine.connect() as conn:
            result = {}
            for table, field, value in (
                ("runs", "run_id", self.record.run_id),
                ("fleet_run_placements", "run_id", self.record.run_id),
                ("fleet_agent_tasks", "id", self.claim["agent_task_id"]),
                ("fleet_attempts", "id", self.claim["attempt_id"]),
            ):
                row = (await conn.execute(text(f"SELECT * FROM {table} WHERE {field}=:id"), {"id": value})).mappings().one()
                result[table] = {key: val for key, val in dict(row).items() if "token" not in key and key not in {"payload", "kwargs_json"}}
            result["stream_seals"] = (await conn.execute(text("SELECT count(*) FROM fleet_stream_seals WHERE run_id=:id"), {"id": self.record.run_id})).scalar_one()
        return result

    async def accepted_file_url(self):
        # Capability inspection is baseline-safe: missing proposed tables means
        # no accepted point. It never creates expected publication data.
        async with self.engine.connect() as conn:
            exists = await conn.scalar(text("SELECT to_regclass('fleet_workspace_points') IS NOT NULL"))
            points = []
            if exists:
                points = [dict(row) for row in (await conn.execute(text("SELECT * FROM fleet_workspace_points WHERE run_id=:id"), {"id": self.record.run_id})).mappings()]
        self.accepted_points = points
        # Future production public-link fields may name a complete owner URL.
        for point in points:
            for key in ("public_url", "file_url", "download_url"):
                if point.get(key):
                    return self.url + point[key]
        return f"{self.url}/api/threads/{self.record.thread_id}/artifacts/mnt/user-data/outputs/partial.txt"


if __name__ == "__main__":
    sys.modules["fleet.c08_integration_fixture"] = sys.modules["__main__"]
    from unittest.mock import patch

    from langchain_core.tools import BaseTool

    actual_invoke = BaseTool.ainvoke
    actual_sync_invoke = BaseTool.invoke

    async def observed_invoke(tool, input, *args, **kwargs):
        owned = tool.name in {"write_file", "present_files", "bash"}
        identity = input.get("id") if isinstance(input, dict) else None
        if owned:
            original.receipt("tool-start", name=tool.name, tool_call_id=identity)
        result = await actual_invoke(tool, input, *args, **kwargs)
        if owned:
            original.receipt("tool-return", name=tool.name, tool_call_id=identity)
        return result

    def observed_sync_invoke(tool, input, *args, **kwargs):
        owned = tool.name in {"write_file", "present_files", "bash"}
        identity = input.get("id") if isinstance(input, dict) else None
        if owned:
            original.receipt("tool-start", name=tool.name, tool_call_id=identity)
        result = actual_sync_invoke(tool, input, *args, **kwargs)
        if owned:
            original.receipt("tool-return", name=tool.name, tool_call_id=identity)
        return result

    with patch.object(BaseTool, "ainvoke", observed_invoke), patch.object(BaseTool, "invoke", observed_sync_invoke):
        asyncio.run(original.child_main())
