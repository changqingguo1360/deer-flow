"""Only model script/append receipt child; original stock bash owns supervision."""

import hashlib
import importlib
import json
import os
import time
from pathlib import Path

from fleet.c09_stock_linux_fixture import BarrierModel, receipt

OUTPUT = Path("/mnt/user-data/outputs/c09-partition-writers.jsonl")
NATURAL_SECONDS = 180


class PartitionModel(BarrierModel):
    def reply(self, messages):
        result = super().reply(messages)  # Original provider/private checks and installed wheel verification.
        for call in result.tool_calls:
            if call["name"] == "bash":
                origins = {}
                for name in (
                    "fleet.c09_stock_linux_partition_fixture",
                    "app.fleet.execution",
                    "app.fleet.agent_control",
                    "app.fleet.runner_context",
                    "app.fleet.mutation",
                    "deerflow_ecs_fleet.worker.agent_runner",
                    "deerflow.runtime.runs.worker",
                    "deerflow.persistence.run.sql",
                ):
                    path = Path(importlib.import_module(name).__file__)
                    origins[name] = {
                        "path": str(path),
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    }
                receipt("c09-partition-installed-origins", modules=origins)
                call["args"]["command"] = (
                    "cat /mnt/user-data/workspace/source.txt /mnt/user-data/uploads/source.txt > /mnt/user-data/outputs/parent.txt"
                    " && python -m fleet.c09_stock_linux_partition_fixture --append --output /mnt/user-data/outputs/c09-partition-writers.jsonl"
                )
                receipt(
                    "c09-partition-original-supervised-bash",
                    tool_call_id=call["id"],
                    natural_seconds=NATURAL_SECONDS,
                )
        return result


def append_receipts(output):
    # Original bash rewrites the virtual --output to its real scoped host path,
    # exactly as the accepted Task3 sideeffect CLI does. Preserve that path.
    if output.name != OUTPUT.name:
        raise ValueError("Original partition receipt filename required")
    chain = []
    pid = os.getpid()
    while pid > 0:
        raw = Path(f"/proc/{pid}/stat").read_text()
        fields = raw[raw.rfind(")") + 2 :].split()
        chain.append(
            {
                "pid": pid,
                "ppid": int(fields[1]),
                "start_ticks": int(fields[19]),
                "cgroup": Path(f"/proc/{pid}/cgroup").read_text(),
            }
        )
        pid = int(fields[1])
    entered = time.monotonic()
    sequence = 0
    while time.monotonic() - entered < NATURAL_SECONDS:
        value = {
            "pid": os.getpid(),
            "process_chain": chain,
            "sequence": sequence,
            "monotonic": time.monotonic(),
            "unix_ns": time.time_ns(),
            "natural_seconds": NATURAL_SECONDS,
        }
        with output.open("a") as file:
            file.write(json.dumps(value, sort_keys=True) + "\n")
            file.flush()
            os.fsync(file.fileno())
        sequence += 1
        time.sleep(0.1)
    raise RuntimeError("Original partition shell survived every bounded safety STOP")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--append", action="store_true", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    append_receipts(args.output)
