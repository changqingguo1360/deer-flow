"""Original empty first-launch staging and original publication; native fd proof only."""

import hashlib
import shutil
from types import SimpleNamespace

import pytest

from .test_c08_workspace import identity


def initial_workspace(tmp_path, *, nonempty=False, owner="user", limit=1024):
    from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceManifest, AgentWorkspaceSnapshots
    from deerflow_ecs_fleet.workspace import NASWorkspace

    boundary = identity(presented_paths=())
    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("native-empty\n")
    data = b"original trusted input"
    files = [{"category": "uploads", "path": "input.txt", "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}] if nonempty else []
    manifest = AgentWorkspaceManifest(user_id=owner, thread_id=boundary.thread_id, files=files, total_bytes=len(data) if files else 0)
    spec = {"user_id": boundary.user_id, "thread_id": boundary.thread_id, "workspace_manifest_ref": manifest.reference}
    source = nas / ".fleet-agent-inputs" / boundary.user_id / boundary.thread_id / manifest.reference
    source.mkdir(parents=True)
    (source / "manifest.json").write_bytes(manifest.canonical_bytes())
    if files:
        (source / "uploads").mkdir()
        (source / "uploads/input.txt").write_bytes(data)
    attempt = nas / boundary.user_id / boundary.thread_id / "agents" / boundary.agent_task_id / boundary.attempt_id
    attempt.mkdir(parents=True)
    snapshots = AgentWorkspaceSnapshots(nas, state_dir=tmp_path / "private-control", max_input_bytes=limit)
    root = attempt / ".deer-flow/users" / boundary.user_id / "threads" / boundary.thread_id / "user-data"
    return SimpleNamespace(boundary=boundary, spec=spec, nas=NASWorkspace(nas, identity="native-empty"), source=source, attempt=attempt, snapshots=snapshots, user_data=root, data=data)


@pytest.mark.parametrize("nonempty", [False, True])
def test_original_first_launch_then_original_publisher_handles_no_sandbox(tmp_path, nonempty):
    from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication

    item = initial_workspace(tmp_path, nonempty=nonempty)
    item.snapshots.prepare(item.spec, item.attempt)
    publisher = AgentWorkspacePublication(client=None, containers=None, nas=item.nas, journal=None)
    grant = {"input_limits": {"max_input_bytes": 1024}, "execution_profile": {"max_output_bytes": 1024}}
    candidate = publisher._candidate(item.boundary, grant, item.attempt)
    assert candidate.categories == ("workspace", "uploads", "outputs")
    assert candidate.total_bytes == (len(item.data) if nonempty else 0)
    assert all((item.user_data / category).is_dir() for category in candidate.categories)
    if nonempty:
        assert (item.user_data / "uploads/input.txt").read_bytes() == item.data
        assert (item.user_data / "uploads/input.txt").stat().st_ino != (item.source / "uploads/input.txt").stat().st_ino


@pytest.mark.parametrize("tamper", ["remove-root", "remove-category", "symlink-root", "symlink-category"])
def test_original_prepared_retry_must_not_recreate_or_follow_tampered_approved_dirs(tmp_path, tamper):
    item = initial_workspace(tmp_path, nonempty=True)
    item.snapshots.prepare(item.spec, item.attempt)
    target = item.user_data if tamper.endswith("root") else item.user_data / "uploads"
    outside = tmp_path / "outside"
    outside.mkdir()
    shutil.rmtree(target)
    if tamper.startswith("symlink"):
        target.symlink_to(outside, target_is_directory=True)
    markers = {p: p.read_bytes() for p in item.snapshots.state_dir.glob("*.json")}
    with pytest.raises((ValueError, OSError)):
        item.snapshots.prepare(item.spec, item.attempt)
    assert {p: p.read_bytes() for p in markers} == markers
    assert list(outside.iterdir()) == []
    assert target.is_symlink() if tamper.startswith("symlink") else not target.exists()


@pytest.mark.parametrize("change", ["owner", "reference", "budget", "root-symlink"])
def test_original_first_launch_invalid_input_never_gets_prepared_marker(tmp_path, change):
    item = initial_workspace(tmp_path, nonempty=True, owner="other-user" if change == "owner" else "user", limit=1 if change == "budget" else 1024)
    if change == "reference":
        item.spec["workspace_manifest_ref"] = "b" * 64
    if change == "root-symlink":
        outside = tmp_path / "outside"
        outside.mkdir()
        (item.attempt / ".deer-flow").symlink_to(outside, target_is_directory=True)
    with pytest.raises((ValueError, OSError)):
        item.snapshots.prepare(item.spec, item.attempt)
    assert not list(item.snapshots.state_dir.glob("*.json"))
    if change == "root-symlink":
        assert list(outside.iterdir()) == []
    else:
        assert not (item.attempt / ".deer-flow").exists()
