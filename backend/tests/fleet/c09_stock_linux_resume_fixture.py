"""Scripted model and graph interrupt only; all execution uses original stock code."""

import asyncio
import hashlib
import importlib
import json
import os
import time
from pathlib import Path

from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.types import interrupt

from deerflow.config.paths import get_paths
from deerflow.tools.builtins.list_uploaded_files_tool import _resolve_thread_id, _resolve_user_id
from deerflow.tools.types import Runtime
from fleet.c04_worker_fixture import ScriptedModel

RECEIPT = Path("/mnt/user-data/outputs/c09-sideeffects.jsonl")


def receipt(event, **fields):
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


@tool
async def request_original_input(runtime: Runtime) -> str:
    """Request human input after the parallel supervised bash receipt exists."""
    thread_id = _resolve_thread_id(runtime)
    if thread_id is None:
        raise RuntimeError("Original tool thread identity required")
    receipt_path = get_paths().sandbox_outputs_dir(thread_id, user_id=_resolve_user_id(runtime)) / RECEIPT.name
    async with asyncio.timeout(20):
        while not receipt_path.exists():
            await asyncio.sleep(0.02)
    observed = json.loads(receipt_path.read_text().splitlines()[0])
    async with asyncio.timeout(20):
        while (process_stat := Path(f"/proc/{observed['pid']}/stat")).exists():
            try:
                raw = process_stat.read_text()
            except FileNotFoundError:
                break
            fields = raw[raw.rfind(")") + 2 :].split()
            if int(fields[19]) != observed["process_chain"][0]["start_ticks"]:
                break  # a new container may reuse the historical PID
            await asyncio.sleep(0.02)
    await asyncio.sleep(0.05)  # allow original bash to collect its exited child
    # This is the real LangGraph interrupt in the original lead ToolNode. The
    # completed parallel bash result remains in the original pending writes.
    receipt("c09-original-tool-interrupt", receipt_sha256=hashlib.sha256(receipt_path.read_bytes()).hexdigest())
    return interrupt({"question": "Continue the original stock graph?"})


class ResumeModel(ScriptedModel):
    def reply(self, messages):
        super().reply(messages)  # Preserve existing private credential/skill checks.
        tools = {message.name: message for message in messages if isinstance(message, ToolMessage)}
        if "request_original_input" not in tools:
            from fleet.c08_installed_bytes import verify_installed

            verify_installed()
            origins = {}
            for name in (
                "app.fleet.execution",
                "app.fleet.ownership",
                "app.fleet.workspace",
                "app.gateway.services",
                "app.fleet.runner_context",
                "deerflow_ecs_fleet.worker.agent_runner",
                "deerflow.runtime.runs.worker",
                "deerflow.persistence.run.sql",
                "deerflow.agents.lead_agent.agent",
                "fleet.c09_stock_linux_resume_fixture",
            ):
                path = Path(importlib.import_module(name).__file__)
                origins[name] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            receipt("c09-resume-installed-origins", modules=origins)
            return AIMessage(
                content="",
                id="c09-resume-parallel",
                tool_calls=[
                    {
                        "id": "c09-original-bash-once",
                        "type": "tool_call",
                        "name": "bash",
                        "args": {
                            "description": "Append one actual supervised process receipt",
                            "command": (
                                "cat /mnt/user-data/workspace/source.txt /mnt/user-data/uploads/source.txt > /mnt/user-data/outputs/parent.txt"
                                " && python -m fleet.c09_stock_linux_resume_fixture --sideeffect --output /mnt/user-data/outputs/c09-sideeffects.jsonl"
                            ),
                        },
                    },
                    {"id": "c09-original-input", "type": "tool_call", "name": "request_original_input", "args": {}},
                ],
            )
        if tools["request_original_input"].content != "approved":
            raise RuntimeError("Original keyed human answer was not materialized")
        if "bash" not in tools:
            raise RuntimeError("Original completed parallel bash cache was lost")
        if "present_files" not in tools:
            return AIMessage(content="", id="c09-resume-present", tool_calls=[{"id": "c09-present", "type": "tool_call", "name": "present_files", "args": {"filepaths": [str(RECEIPT), "/mnt/user-data/outputs/parent.txt"]}}])
        receipt("c09-original-resumed-model-complete", cached_bash_tool_call_id=tools["bash"].tool_call_id, keyed_answer=tools["request_original_input"].content)
        return AIMessage(content="Original keyed stock graph completed", id="c09-resume-complete")


def sideeffect(output: Path):
    # The original supervised bash launches this actual child. Each invocation
    # appends a real process receipt; the test counts the received file lines.
    chain = []
    pid = os.getpid()
    while pid > 0:
        raw = Path(f"/proc/{pid}/stat").read_text()
        fields = raw[raw.rfind(")") + 2 :].split()
        chain.append({"pid": pid, "ppid": int(fields[1]), "start_ticks": int(fields[19])})
        pid = int(fields[1])
    value = {"pid": os.getpid(), "process_chain": chain, "monotonic": time.monotonic()}
    with output.open("a") as file:
        file.write(json.dumps(value, sort_keys=True) + "\n")
        file.flush()
        os.fsync(file.fileno())
    receipt("c09-original-supervised-sideeffect", **value)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--sideeffect", required=True, action="store_true")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    sideeffect(args.output)
