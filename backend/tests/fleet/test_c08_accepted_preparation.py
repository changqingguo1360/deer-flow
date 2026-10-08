"""Actual Agent container preparation consumes complete accepted C content."""

import os

import pytest

from .test_c08_workspace import seal
from .test_c08_workspace import storage as storage


@pytest.mark.asyncio
async def test_actual_agent_preparation_clones_accepted_all_categories_and_reverifies_retry(storage, tmp_path):
    from deerflow_ecs_fleet.worker.agent_containers import AgentContainers

    store, source = storage
    (source / "workspace/input").write_bytes(b"workspace")
    (source / "uploads/input").write_bytes(b"upload")
    manifest = seal(store, source)
    containers = AgentContainers(state_dir=tmp_path / "node-state", operator_config={})
    spec = dict(user_id="user", thread_id="thread", agent_task_id="task", workspace_manifest_ref="a" * 64)
    claim = dict(attempt_id="new-attempt")
    grant = dict(
        launch_spec=spec,
        input_limits={"max_input_bytes": 1024},
        execution_profile={"max_output_bytes": 1024},
        accepted_workspace=dict(point_id="request", checkpoint_id="checkpoint", manifest=manifest.model_dump(mode="json")),
        nas_identity="test",
    )
    target = await containers.prepare_workspace(store.nas.root, claim, grant)
    data = target / ".deer-flow/users/user/threads/thread/user-data"
    for name in ("workspace/input", "uploads/input", "outputs/result"):
        original = source / name
        sealed = store.nas.root / manifest.nas_prefix / name
        restored = data / name
        assert restored.read_bytes() == original.read_bytes()
        assert len({(p.stat().st_dev, p.stat().st_ino) for p in (original, sealed, restored)}) == 3
        assert restored.stat().st_nlink == 1
    assert await containers.prepare_workspace(store.nas.root, claim, grant) == target
    (store.nas.root / manifest.nas_prefix / "outputs/result").unlink()
    with pytest.raises((ValueError, OSError)):
        await containers.prepare_workspace(store.nas.root, claim, grant)
    assert (data / "outputs/result").read_bytes() == b"content"


def test_initial_marker_retry_reverifies_original_source_without_overwriting_runtime(tmp_path):
    import hashlib

    from deerflow_ecs_fleet.worker.agent_workspace import AgentWorkspaceManifest, AgentWorkspaceSnapshots

    content = b"original"
    manifest = AgentWorkspaceManifest(user_id="user", thread_id="thread", files=[dict(category="uploads", path="input", size=len(content), sha256=hashlib.sha256(content).hexdigest())], total_bytes=len(content))
    spec = dict(user_id="user", thread_id="thread", workspace_manifest_ref=manifest.reference)
    nas = tmp_path / "nas"
    root = nas / ".fleet-agent-inputs/user/thread" / manifest.reference
    (root / "uploads").mkdir(parents=True)
    (root / "manifest.json").write_bytes(manifest.canonical_bytes())
    (root / "uploads/input").write_bytes(content)
    target = tmp_path / "attempt"
    target.mkdir()
    snapshots = AgentWorkspaceSnapshots(nas, state_dir=tmp_path / "private", max_input_bytes=1024)
    snapshots.prepare(spec, target)
    copied = target / ".deer-flow/users/user/threads/thread/user-data/uploads/input"
    copied.write_bytes(b"runtime changed")
    snapshots.prepare(spec, target)
    assert copied.read_bytes() == b"runtime changed"
    (root / "uploads/input").unlink()
    with pytest.raises((ValueError, OSError)):
        snapshots.prepare(spec, target)


def test_verified_output_holds_original_bytes_across_nas_mutation_and_rejects_same_stat(storage):
    store, source = storage
    manifest = seal(store, source)
    selected = store.nas.root / manifest.nas_prefix / "outputs/result"
    original_stat = selected.stat()
    descriptor, item = store.open_verified_output(manifest, "outputs/result")
    try:
        selected.write_bytes(b"CONTENT")
        os.utime(selected, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
        assert descriptor.read() == b"content"
        with pytest.raises((ValueError, OSError)):
            store.open_verified_output(manifest, "outputs/result")
    finally:
        descriptor.close()


@pytest.mark.asyncio
async def test_cancelled_branch_restore_waits_for_worker_then_removes_private_clone(storage, tmp_path, monkeypatch):
    import asyncio
    import threading

    import app.fleet.workspace_files as module
    from app.fleet.workspace_files import FleetWorkspaceFiles

    store, source = storage
    manifest = seal(store, source)
    service = FleetWorkspaceFiles.__new__(FleetWorkspaceFiles)
    service.versions = store
    target = tmp_path / "cancelled-child"
    entered, cleaned, settling = asyncio.Event(), asyncio.Event(), asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original_settle = module._settle_owned

    async def observed_settle(task):
        settling.set()
        return await original_settle(task)

    monkeypatch.setattr(module, "_settle_owned", observed_settle)
    original_restore = store.restore
    original_remove = module.shutil.rmtree

    def held_restore(value, fd):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(3)
        original_restore(value, fd)

    def observed_remove(path, *args, **kwargs):
        original_remove(path, *args, **kwargs)
        loop.call_soon_threadsafe(cleaned.set)

    monkeypatch.setattr(store, "restore", held_restore)
    monkeypatch.setattr(module.shutil, "rmtree", observed_remove)
    pending = asyncio.create_task(service.restore_branch(manifest, target))
    await asyncio.wait_for(entered.wait(), 2)
    pending.cancel()
    try:
        observation = asyncio.create_task(settling.wait())
        await asyncio.wait({observation, pending}, timeout=2, return_when=asyncio.FIRST_COMPLETED)
        assert not pending.done(), "Cancel escaped while the original copy worker still owned the clone"
        assert observation.done(), "Original cancellation owner never entered settlement"
        pending.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert cleaned.is_set(), "Request cancellation returned before owned cleanup settled"
        assert not target.exists()
    finally:
        release.set()
