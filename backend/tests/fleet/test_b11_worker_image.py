"""Built worker runtime and rendered compose have no inbound published ports."""

import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_frozen_worker_image_has_real_linux_docker_control():
    image = os.environ.get("FLEET_TEST_WORKER_IMAGE")
    if not image or os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires an explicitly built worker content ID and local Docker")
    from .test_b06_containers import docker

    inspect = json.loads(await docker("image", "inspect", image))[0]
    assert image in {inspect["Id"], *inspect.get("RepoDigests", [])}
    assert inspect["Os"] == "linux"
    assert inspect["Config"]["Entrypoint"] == ["python", "-m", "deerflow_ecs_fleet.worker"]
    assert not inspect["Config"].get("ExposedPorts")
    help_text = await docker("run", "--rm", "--network", "none", image, "--help")
    assert "--settings" in help_text
    name = "fleet-b11-image-" + uuid4().hex
    script = (
        "import asyncio,json; from pathlib import Path; from deerflow_ecs_fleet.worker.containers import DockerContainers; c=DockerContainers(state_dir=Path('/unused')); print(json.dumps(asyncio.run(c.inspect('"
        + name
        + "'))['HostConfig']['PortBindings'])); print(asyncio.run(c.checked('--version')))"
    )
    socket = os.environ.get("FLEET_TEST_DOCKER_SOCKET", "/var/run/docker.sock")
    try:
        observed = await docker("run", "--name", name, "--network", "none", "--read-only", "--mount", "type=bind,src=" + socket + ",dst=/var/run/docker.sock", "--entrypoint", "python", image, "-c", script)
        lines = observed.splitlines()
        assert json.loads(lines[0]) in ({}, None)
        assert lines[1].startswith("Docker version ")
        actual = json.loads(await docker("inspect", name))[0]
        assert actual["State"]["ExitCode"] == 0
        assert actual["HostConfig"]["PortBindings"] in ({}, None)
    finally:
        await docker("rm", "-f", name)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rendered_compose_preserves_host_nas_path_and_no_ports(tmp_path):
    if os.environ.get("FLEET_TEST_CONTAINERS") != "1":
        pytest.skip("requires explicit local Docker compose gate")
    root = Path(__file__).resolve().parents[3]
    values = {
        "FLEET_WORKER_IMAGE": "fleet@sha256:" + "a" * 64,
        "FLEET_WORKER_BASE": "python@sha256:" + "b" * 64,
        "FLEET_DOCKER_CLI_SHA256": "c" * 64,
        "FLEET_BUILD_ARTIFACTS": str(tmp_path / "artifacts"),
        "FLEET_WORKER_SETTINGS": str(tmp_path / "worker.json"),
        "FLEET_WORKER_CREDENTIAL": str(tmp_path / "credential"),
        "FLEET_WORKER_STATE": str(tmp_path / "private"),
        "FLEET_NAS_ROOT": str(tmp_path / "nas"),
    }
    proc = await asyncio.create_subprocess_exec(
        "docker", "compose", "--profile", "fleet-worker", "-f", str(root / "docker/fleet/compose.yaml"), "config", "--format", "json", env=os.environ | values, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), 10)
    assert proc.returncode == 0, stderr.decode()
    worker = json.loads(stdout)["services"]["worker"]
    assert worker.get("ports", []) == []
    assert worker.get("expose", []) == []
    assert worker["restart"] == "no"
    nas = next(v for v in worker["volumes"] if v["target"] == values["FLEET_NAS_ROOT"])
    assert nas["source"] == nas["target"]
    assert all(not v["bind"]["create_host_path"] for v in worker["volumes"])
