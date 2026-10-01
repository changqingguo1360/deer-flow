"""Real containers prove no duplicate start and actual stop on local lease loss."""

import asyncio
import importlib
import importlib.util
import json
import os
from uuid import uuid4

import pytest


async def docker(*args):
    proc = await asyncio.create_subprocess_exec("docker", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
    assert proc.returncode == 0, stderr.decode()
    return stdout.decode().strip()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_container_launch_once_and_stop_on_lease_loss(tmp_path):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit FLEET_TEST_CONTAINERS=1 local Docker gate")
    assert importlib.util.find_spec("deerflow_ecs_fleet.worker") is not None, "Fleet worker package missing"
    assert importlib.util.find_spec("deerflow_ecs_fleet.worker.containers") is not None, "Fleet container control missing"
    from deerflow_ecs_fleet.config import ExecutionProfile

    containers_cls = importlib.import_module("deerflow_ecs_fleet.worker.containers").DockerContainers
    watchdog_cls = importlib.import_module("deerflow_ecs_fleet.worker.watchdog").LeaseWatchdog
    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    attempt_id = str(uuid4())
    ref = "fleet-" + attempt_id
    output = tmp_path / "output"
    output.mkdir(mode=0o777)
    output.chmod(0o777)
    state = tmp_path / "private-worker-state"
    profile = ExecutionProfile(image=image, cpu_millis=1000, memory_mib=512)
    grant = {
        "authorized": True,
        "attempt_id": attempt_id,
        "process_ref": ref,
        "lease_seconds_remaining": 10,
        "execution_seconds_remaining": 10,
        "launch_spec": {"profile": profile.model_dump(), "spec": {"argv": ["sh", "-c", "echo start >> /output/count; while true; do echo tick >> /output/ticks; sleep .1; done"]}},
    }
    containers = containers_cls(state_dir=state)
    try:
        await containers.launch(grant, output_dir=output)
        await containers.launch(grant, output_dir=output)
        for _ in range(60):
            if (output / "count").exists() and (output / "ticks").exists():
                break
            await asyncio.sleep(0.05)
        assert (output / "count").read_text().splitlines() == ["start"]
        inspection = json.loads(await docker("inspect", ref))[0]
        assert inspection["State"]["Running"]
        assert inspection["HostConfig"]["ReadonlyRootfs"]
        assert inspection["HostConfig"]["NetworkMode"] == "none"
        assert inspection["HostConfig"]["CapDrop"] == ["ALL"]
        assert inspection["Config"]["User"] == "65534:65534"
        assert [m["Destination"] for m in inspection["Mounts"]] == ["/output"]
        watchdog = watchdog_cls(stop=lambda: containers.stop(ref), lease_seconds=0.4, safety_margin_seconds=0.1)
        assert await asyncio.wait_for(watchdog.run(), timeout=5)
        assert not json.loads(await docker("inspect", ref))[0]["State"]["Running"]
        ticks = (output / "ticks").read_text()
        await asyncio.sleep(0.3)
        assert (output / "ticks").read_text() == ticks
        restarted = containers_cls(state_dir=state)
        await restarted.launch(grant, output_dir=output)
        assert (output / "count").read_text().splitlines() == ["start"]
        invalid = grant | {"authorized": False, "attempt_id": str(uuid4()), "process_ref": "fleet-" + str(uuid4())}
        with pytest.raises(ValueError, match="authoriz"):
            await restarted.launch(invalid, output_dir=output)
    finally:
        await docker("rm", "-f", ref)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stop_refuses_unmanaged_container(tmp_path):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit local Docker gate")
    from deerflow_ecs_fleet.worker.containers import DockerContainers, DockerError

    image = await docker("image", "inspect", "alpine:3.20", "--format", "{{.Id}}")
    ref = "fleet-" + str(uuid4())
    await docker("run", "-d", "--name", ref, "--network=none", image, "sleep", "30")
    try:
        with pytest.raises(DockerError, match="managed"):
            await DockerContainers(state_dir=tmp_path / "state").stop(ref)
        assert json.loads(await docker("inspect", ref))[0]["State"]["Running"]
    finally:
        await docker("rm", "-f", ref)
