"""Installed non-Gateway compatibility fixture; never imports app or harness."""

from hashlib import sha256
from pathlib import Path


async def factory(*, bootstrap, spec, grant):
    raise RuntimeError("This fixture only exercises nonsecret installed provider preflight")


def compatibility():
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility

    return WorkerCompatibility(runtime_digest="sha256:" + sha256(Path(__file__).read_bytes()).hexdigest(), skill_snapshot={"entries": []}, plugin_snapshot={"entries": []})


factory.worker_compatibility = compatibility


async def unfit(*, bootstrap, spec, grant):
    return None


async def invalid(*, bootstrap, spec, grant):
    return None


invalid.worker_compatibility = lambda: {"runtime_digest": "unfit"}

sentinel = 42
