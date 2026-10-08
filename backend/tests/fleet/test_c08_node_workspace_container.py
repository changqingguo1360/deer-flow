"""Native Docker-argument/CLI-owner contracts; receipts are explicit fixtures.

No test here executes Docker, Linux collector, or proves container quiescence.
"""

import asyncio
import copy
import hashlib
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

import pytest

from .test_c02_remote_agent_admission import admission as admission  # noqa: F401
from .test_c03_remote_agent_admission import owner_environment as owner_environment  # noqa: F401
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner  # noqa: F401
from .test_c08_node_workspace_protocol import publish_original


def container_observation(grant, output):
    profile = grant["execution_profile"]
    fingerprint = hashlib.sha256(json.dumps([grant["launch_spec"], profile, str(output)], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {
        "Id": "a" * 64,
        "Image": profile["image"].rsplit("@", 1)[-1],
        "State": {"Running": True, "Paused": False, "StartedAt": "2026-10-03T01:00:00.000000000Z"},
        "Config": {
            "Image": profile["image"],
            "User": profile["user"],
            "Entrypoint": ["python"],
            "Cmd": ["-I", "-S", "/opt/deerflow/libexec_bootstrap.py", "--provider", "gateway"],
            "WorkingDir": "/workspace",
            "Labels": {"deerflow.fleet.attempt": grant["attempt_id"], "deerflow.fleet.node": grant["node_id"], "deerflow.fleet.launch": fingerprint},
        },
        "HostConfig": {
            "ReadonlyRootfs": True,
            "PidsLimit": profile["pids_limit"],
            "Memory": profile["memory_mib"] * 1048576,
            "NanoCpus": profile["cpu_millis"] * 1000000,
            "NetworkMode": profile["network"],
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges"],
            "RestartPolicy": {"Name": "no"},
            "Privileged": False,
            "CapAdd": None,
            "PidMode": "",
            "UsernsMode": "",
            "CgroupnsMode": "private",
            "CgroupParent": "",
        },
        "Mounts": [{"Type": "bind", "Source": str(output), "Destination": "/workspace", "RW": True}],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_after,duplicate_receipt", ((False, False), (True, False), (False, True)))
async def test_native_exact_container_exec_and_actual_cli_join_contract(checkpoint_owner, tmp_path, monkeypatch, changed_after, duplicate_receipt):  # noqa: F811
    from deerflow_ecs_fleet.worker import agent_containers as module
    from deerflow_ecs_fleet.worker import workspace_collector
    from deerflow_ecs_fleet.worker.containers import DockerError

    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    output = tmp_path / "mounted-attempt"
    output.mkdir()
    containers = module.AgentContainers(state_dir=tmp_path / "private-node", operator_config={})
    containers.bind_claim({"attempt_id": identity.attempt_id, "token": item.accepted.token})
    observation = container_observation(item.grant, output)
    inspected, actual_cli = [], []

    async def inspect(ref):
        inspected.append(ref)
        value = copy.deepcopy(observation)
        if len(inspected) > 1 and changed_after:
            value["State"]["StartedAt"] = "2026-10-03T02:00:00.000000000Z"
        return value

    monkeypatch.setattr(containers, "inspect", inspect)
    original_spawn = asyncio.create_subprocess_exec
    digest = hashlib.sha256(Path(workspace_collector.__file__).read_bytes()).hexdigest()
    script = (
        "import sys,json; r=json.load(sys.stdin); "
        "p={'pid':99,'ppid':0,'start_ticks':990,'pid_namespace':77,'state':'R','uid':0}; "
        "root={'pid':1,'ppid':0,'start_ticks':10,'state':'S','uid':int(r['workload_user'].split(':')[0])}; "
        "print(json.dumps({'nonce':r['nonce'],'request_digest':r['request_digest'],'barrier_epoch':r['barrier_epoch'],"
        "'collector':p,'runner':root,'remaining_pids':[1,99],'collector_digest':sys.argv[1],'cgroup_digest':'a'*64}))"
    )

    if duplicate_receipt:
        script = script.replace("print(json.dumps(", "print('{\"nonce\":\"' + r['nonce'] + '\",' + json.dumps(")
        script = script[:-2] + ")[1:])"

    async def native_cli(*args, **kwargs):
        assert args == ("docker", "exec", "-i", "--user", "0:0", "a" * 64, "python", "-I", "-S", workspace_collector.COLLECTOR_PATH)
        child = await original_spawn(sys.executable, "-c", script, digest, **kwargs)
        actual_cli.append(child)
        return child

    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", native_cli)
    request = {"identity": json.loads(json.dumps(asdict(identity))), "request_digest": identity.request_digest, "barrier_epoch": epoch, "processes": [], "state": "sealing", "nonce": "e" * 64}
    try:
        if changed_after or duplicate_receipt:
            with pytest.raises(DockerError):
                await containers.quiesce_workspace(item.grant, output_dir=output, request=request, deadline=time.monotonic() + 3)
        else:
            receipt = await containers.quiesce_workspace(item.grant, output_dir=output, request=request, deadline=time.monotonic() + 3)
            assert receipt["container_id"] == "a" * 64
            assert receipt["receipt"]["request_digest"] == identity.request_digest
            assert teardown.budget._deadline is None
        assert len(actual_cli) == 1 and actual_cli[0].returncode == 0
        assert inspected == (["fleet-" + identity.attempt_id] if duplicate_receipt else ["fleet-" + identity.attempt_id, "a" * 64])
        assert not containers._workspace_readers
    finally:
        await teardown.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("unsafe", ({"Privileged": True}, {"CapAdd": ["SYS_PTRACE"]}, {"PidMode": "host"}, {"UsernsMode": "host"}, {"CgroupnsMode": "host"}))
async def test_frozen_container_namespace_and_privilege_changes_reject_before_exec(checkpoint_owner, tmp_path, monkeypatch, unsafe):  # noqa: F811
    from deerflow_ecs_fleet.worker import agent_containers as module
    from deerflow_ecs_fleet.worker.containers import DockerError

    item = checkpoint_owner
    identity, epoch, teardown = await publish_original(item)
    output = tmp_path / "mounted-attempt"
    output.mkdir()
    containers = module.AgentContainers(state_dir=tmp_path / "private", operator_config={})
    containers.bind_claim({"attempt_id": identity.attempt_id, "token": item.accepted.token})
    observation = container_observation(item.grant, output)
    observation["HostConfig"].update(unsafe)

    async def inspect(ref):
        return observation

    async def forbidden_exec(*args, **kwargs):
        pytest.fail("Unsafe frozen containment reached a collector exec")

    monkeypatch.setattr(containers, "inspect", inspect)
    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", forbidden_exec)
    request = {"identity": json.loads(json.dumps(asdict(identity))), "request_digest": identity.request_digest, "barrier_epoch": epoch, "processes": [], "state": "sealing", "nonce": "e" * 64}
    try:
        with pytest.raises(DockerError):
            await containers.quiesce_workspace(item.grant, output_dir=output, request=request, deadline=time.monotonic() + 1)
        assert not containers._workspace_execs
    finally:
        await teardown.close()
