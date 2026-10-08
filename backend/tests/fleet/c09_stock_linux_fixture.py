"""Only scripted model/barrier; use the original installed gateway provider."""

import hashlib
import importlib
import json
import os
import time
from pathlib import Path

from fleet.c04_worker_fixture import ScriptedModel


def receipt(event, **fields):
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


class BarrierModel(ScriptedModel):
    def reply(self, messages):
        result = super().reply(messages)  # original credential/skill/private-scope checks
        race = next((mode for mode in ("cancel-first", "completion-first") if any("c09-cas=" + mode in str(message.content) for message in messages)), None)
        if race is not None:
            install_cas_observer(race)
            from langchain_core.messages import AIMessage, ToolMessage

            names = {message.name for message in messages if isinstance(message, ToolMessage)}
            if "bash" in names:
                if "present_files" not in names:
                    return AIMessage(content="", id="c09-cas-present", tool_calls=[{"id": "c09-cas-present-parent", "type": "tool_call", "name": "present_files", "args": {"filepaths": ["/mnt/user-data/outputs/parent.txt"]}}])
                return AIMessage(content="Original supervised artifact completed", id="c09-cas-final")
        for call in result.tool_calls:
            if call["name"] == "bash":
                from fleet.c08_installed_bytes import verify_installed

                verify_installed()
                origins = {}
                for name in (
                    "app.fleet.agent_control",
                    "app.fleet.runner_context",
                    "app.fleet.workspace",
                    "deerflow_ecs_fleet.worker.agent_runner",
                    "deerflow.runtime.runs.worker",
                    "deerflow.runtime.runs.manager",
                    "deerflow.persistence.run.sql",
                    "deerflow.agents.lead_agent.agent",
                ):
                    path = Path(importlib.import_module(name).__file__)
                    origins[name] = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                receipt("c09-original-installed-origins", modules=origins)
                call["args"]["command"] = "cat /mnt/user-data/workspace/source.txt /mnt/user-data/uploads/source.txt > /mnt/user-data/outputs/parent.txt; python -m fleet.c09_stock_linux_fixture --barrier"
                if race is not None:
                    call["args"]["command"] = "cat /mnt/user-data/workspace/source.txt /mnt/user-data/uploads/source.txt > /mnt/user-data/outputs/parent.txt"
                receipt("c09-original-model-supervised-bash", tool_call_id=call["id"])
        return result


def barrier():
    chain = []
    pid = os.getpid()
    while pid > 0:
        raw = Path(f"/proc/{pid}/stat").read_text()
        fields = raw[raw.rfind(")") + 2 :].split()
        chain.append({"pid": pid, "ppid": int(fields[1]), "start_ticks": int(fields[19]), "cgroup": Path(f"/proc/{pid}/cgroup").read_text()})
        pid = int(fields[1])
    Path("/tmp/c09-shell-barrier.json").write_text(json.dumps({"pid": os.getpid(), "process_chain": chain, "entered_monotonic": time.monotonic(), "bounded_tool_seconds": 90}))
    time.sleep(90)  # original supervised tool must be cancelled long before natural end
    raise RuntimeError("C09 HTTP interrupt never cancelled the actual supervised shell")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--barrier", action="store_true", required=True)
    parser.parse_args()
    barrier()


_CAS_CONTEXTS = {}
_CAS_ORIGINAL = None


def install_cas_observer(order):
    """Exact fixture identity wrapper; original SQL/result/cancellation unchanged."""
    import asyncio
    from dataclasses import asdict

    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime.execution.mutation_context import current_remote_mutation_context

    global _CAS_ORIGINAL
    context = current_remote_mutation_context()
    if context is None:
        raise RuntimeError("Original remote fixture context required")
    previous = _CAS_CONTEXTS.setdefault(context.run_id, (context, order))
    if previous != (context, order):
        raise RuntimeError("CAS fixture identity changed")
    if _CAS_ORIGINAL is not None:
        return
    _CAS_ORIGINAL = RunRepository.finalize_if_not_cancelled
    calls = set()

    async def observed(repository, run_id, *args, **kwargs):
        selected = _CAS_CONTEXTS.get(run_id)
        if selected is None:
            return await _CAS_ORIGINAL(repository, run_id, *args, **kwargs)
        expected, mode = selected
        terminal = repository._terminal_participant
        if current_remote_mutation_context() != expected or terminal.capability.context != expected:
            raise RuntimeError("CAS fixture original identity mismatch")
        if run_id in calls:
            raise RuntimeError("Original CAS called more than once")
        calls.add(run_id)
        identity, candidate, epoch = terminal.prepared
        receipt("c09-original-cas-prepared-outcome", run_id=run_id, attempt_id=expected.attempt_id, desired_core_status=identity.desired_core_status, desired_core_error=identity.error)
        if identity.desired_core_status != "success":
            raise RuntimeError("Actual prepared success required")

        async def hold(phase, result=None):
            proof = {
                "event": "c09-original-cas-barrier",
                "phase": phase,
                "order": mode,
                "identity": asdict(identity),
                "candidate": candidate.model_dump(mode="json"),
                "barrier_epoch": epoch,
                "entered_monotonic": time.monotonic(),
                "finalized": None if result is None else result.finalized,
            }
            target = Path("/tmp/c09-cas-" + expected.attempt_id + ".json")
            temporary = target.with_suffix(".pending")
            temporary.write_text(json.dumps(proof, default=str))
            temporary.replace(target)
            receipt("c09-original-cas-barrier", **{key: value for key, value in proof.items() if key != "event"})
            release = Path("/tmp/c09-cas-" + expected.attempt_id + ".release")
            async with asyncio.timeout_at(min(terminal.controller.execution_deadline, time.monotonic() + 30)):
                while not release.exists():
                    await asyncio.sleep(0.02)

        if mode == "cancel-first":
            await hold("before-original-cas")
        result = await _CAS_ORIGINAL(repository, run_id, *args, **kwargs)
        if mode == "completion-first":
            if not result.finalized or result.cancel_action is not None:
                raise RuntimeError("Original completion did not commit")
            await hold("after-original-commit", result)
        receipt("c09-original-cas-return", run_id=run_id, attempt_id=expected.attempt_id, finalized=result.finalized, cancel_action=result.cancel_action)
        return result

    RunRepository.finalize_if_not_cancelled = observed
