"""Descriptor-level C snapshots; no B manifests or runner lifecycle substitutes."""

import importlib
import importlib.util
import os
from dataclasses import replace

import pytest


def api():
    assert importlib.util.find_spec("deerflow_ecs_fleet.agent_workspace") is not None, "C immutable workspace storage missing"
    return importlib.import_module("deerflow_ecs_fleet.agent_workspace")


def identity(**changes):
    from deerflow.runtime.execution.mutation_context import RemoteMutationContext

    context = RemoteMutationContext(
        user_id="user",
        thread_id="thread",
        run_id="run",
        agent_task_id="task",
        generation=1,
        node_id="node",
        node_session_id="session",
        attempt_id="attempt",
        owner_worker_id="fleet-agent:attempt",
        token_stamp="a" * 64,
        launch_spec_digest="sha256:" + "b" * 64,
    )
    return api().WorkspaceBoundaryIdentity.from_context(
        context, **(dict(request_id="request", checkpoint_id="checkpoint", kind="partial", publication_key="key", presented_paths=("outputs/result",), source_workspace_version="initial") | changes)
    )


@pytest.fixture
def storage(tmp_path):
    module = api()
    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("test\n")
    source = tmp_path / "source"
    source.mkdir()
    for category in ("workspace", "uploads", "outputs"):
        (source / category).mkdir()
    (source / "outputs/result").write_bytes(b"content")
    from deerflow_ecs_fleet.workspace import NASWorkspace

    return module.AgentWorkspaceVersions(NASWorkspace(nas, identity="test"), max_input_bytes=1024, max_output_bytes=1024), source


def seal(store, source, boundary=None):
    fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        return store.seal(boundary or identity(), fd)
    finally:
        os.close(fd)


def test_roundtrip_complete_categories_immutable_and_idempotent(storage, tmp_path):
    store, source = storage
    (source / "workspace/empty").mkdir()
    point = seal(store, source)
    assert point == seal(store, source)
    assert point.total_bytes == 7 and point.categories == ("workspace", "uploads", "outputs")
    assert "workspace/empty" in point.directories
    assert point.nas_prefix.startswith(".fleet-agent-workspaces/user/thread/task/attempt/")
    assert isinstance(point.files, tuple) and isinstance(point.directories, tuple)
    with pytest.raises(Exception):
        point.files[0].size = 999
    target = tmp_path / "restore"
    target.mkdir()
    fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY)
    try:
        store.restore(point, fd)
        store.restore(point, fd)
        assert (target / "outputs/result").read_bytes() == b"content"
        assert (target / "workspace/empty").is_dir() and (target / "uploads").is_dir()
        (target / "outputs/old-deleted").write_text("old")
        with pytest.raises(ValueError):
            store.restore(point, fd)
    finally:
        os.close(fd)
    (source / "outputs/result").write_text("changed")
    with pytest.raises(ValueError, match="conflict"):
        seal(store, source)
    assert store.verify(point) == point


@pytest.mark.parametrize("unsafe", ["symlink", "hardlink", "fifo", "depth", "entries", "input_bytes", "output_bytes"])
def test_reject_unsafe_or_unbounded_sources(storage, unsafe):
    store, source = storage
    if unsafe == "symlink":
        (source / "workspace/link").symlink_to(source / "outputs/result")
    elif unsafe == "hardlink":
        os.link(source / "outputs/result", source / "workspace/link")
    elif unsafe == "fifo":
        os.mkfifo(source / "workspace/fifo")
    elif unsafe == "depth":
        folder = source / "workspace"
        for _ in range(65):
            folder = folder / "d"
            folder.mkdir()
    elif unsafe == "entries":
        for i in range(4097):
            (source / "workspace" / str(i)).touch()
    elif unsafe == "input_bytes":
        (source / "workspace/large").write_bytes(b"x" * 1025)
    else:
        (source / "outputs/large").write_bytes(b"x" * 1025)
    with pytest.raises((ValueError, OSError)):
        seal(store, source)


@pytest.mark.parametrize("mutation", ["digest", "delete", "extra_file", "extra_dir", "manifest", "sentinel"])
def test_verify_and_each_restore_retry_reject_mutation(storage, tmp_path, mutation):
    store, source = storage
    point = seal(store, source)
    root = store.nas.root / point.nas_prefix
    if mutation == "digest":
        (root / "outputs/result").write_bytes(b"CONTENT")
    elif mutation == "delete":
        (root / "outputs/result").unlink()
    elif mutation == "extra_file":
        (root / "outputs/extra").touch()
    elif mutation == "extra_dir":
        (root / "uploads/extra").mkdir()
    elif mutation == "manifest":
        (root / "manifest.json").write_text("{}")
    else:
        (store.nas.root / ".deerflow-fleet-root").write_text("other\n")
    with pytest.raises((ValueError, OSError)):
        store.verify(point)
    target = tmp_path / "restore"
    target.mkdir()
    fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises((ValueError, OSError)):
            store.restore(point, fd)
    finally:
        os.close(fd)


def test_replacement_during_copy_rejected(storage, monkeypatch):
    store, source = storage
    original = os.read
    changed = False

    def reading(fd, n):
        nonlocal changed
        chunk = original(fd, n)
        if chunk == b"content" and not changed:
            changed = True
            (source / "outputs/result").unlink()
            (source / "outputs/result").write_bytes(b"content")
        return chunk

    monkeypatch.setattr(os, "read", reading)
    with pytest.raises(ValueError, match="changed|replacement"):
        seal(store, source)


def test_concurrent_identical_publication_no_overwrite(storage):
    from concurrent.futures import ThreadPoolExecutor

    store, source = storage
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: seal(store, source), range(2)))
    assert results[0] == results[1]


def test_boundary_reuses_context_without_secret_serialization():
    boundary = identity()
    assert boundary.checkpoint_ns == ""
    assert "token_stamp" not in boundary.canonical_bytes().decode()
    assert boundary.request_digest == identity().request_digest
    assert replace(boundary, checkpoint_id="other").request_digest != boundary.request_digest


def test_original_execution_stamp_and_process_exact_binding():
    boundary = identity()
    assert replace(boundary, token_stamp="c" * 64).request_digest != boundary.request_digest
    with pytest.raises(ValueError):
        replace(boundary, process_ref="different-process")
    for paths in [("workspace/a",), ("uploads/a",), ("outputs",)]:
        with pytest.raises(ValueError):
            identity(presented_paths=paths)


def test_new_boundary_deleted_file_does_not_resurrect_and_no_alias(storage, tmp_path):
    store, source = storage
    old = seal(store, source)
    (source / "outputs/result").unlink()
    new = seal(store, source, identity(request_id="next", checkpoint_id="next", presented_paths=()))
    assert new.files == () and store.verify(old).files
    target = tmp_path / "clean"
    target.mkdir()
    fd = os.open(target, os.O_RDONLY | os.O_DIRECTORY)
    try:
        store.restore(new, fd)
    finally:
        os.close(fd)
    assert not (target / "outputs/result").exists()
    source_file = source / "workspace/file"
    source_file.write_text("data")
    latest = seal(store, source, identity(request_id="third", checkpoint_id="third", presented_paths=()))
    target2 = tmp_path / "second"
    target2.mkdir()
    fd = os.open(target2, os.O_RDONLY | os.O_DIRECTORY)
    try:
        store.restore(latest, fd)
    finally:
        os.close(fd)
    versions = [source_file, store.nas.root / latest.nas_prefix / "workspace/file", target2 / "workspace/file"]
    assert len({(p.stat().st_dev, p.stat().st_ino) for p in versions}) == 3
    assert all(p.stat().st_nlink == 1 for p in versions)


def test_scandir_collection_is_bounded(storage, monkeypatch):
    module = api()
    seen = []

    class Entries:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def __iter__(self):
            return self

        def __next__(self):
            from types import SimpleNamespace

            seen.append(len(seen))
            return SimpleNamespace(name=str(len(seen)))

    monkeypatch.setattr(os, "scandir", lambda fd: Entries())
    with pytest.raises(ValueError):
        module.bounded_names(123, 4096)
    assert len(seen) == 4097


def test_changed_boundary_identity_same_publication_conflicts(storage):
    store, source = storage
    point = seal(store, source)
    changed = identity(request_id="changed", source_workspace_version="another")
    with pytest.raises(ValueError, match="conflict"):
        seal(store, source, changed)
    assert store.verify(point) == point


def test_root_directory_replacement_during_copy_rejected(storage, monkeypatch):
    store, source = storage
    original = os.read
    changed = False

    def reading(fd, n):
        nonlocal changed
        data = original(fd, n)
        if data == b"content" and not changed:
            changed = True
            source.rename(source.with_name("replaced"))
            source.mkdir()
        return data

    monkeypatch.setattr(os, "read", reading)
    with pytest.raises(ValueError, match="changed"):
        seal(store, source)


def test_submitted_model_copy_cannot_select_arbitrary_nas_prefix(storage):
    store, source = storage
    point = seal(store, source)
    altered = point.model_copy(update={"nas_prefix": "user/thread/jobs/task/attempts/attempt/sealed"})
    with pytest.raises(ValueError):
        store.verify(altered)


def test_earlier_file_replaced_while_later_file_copies_rejected(storage, monkeypatch):
    store, source = storage
    (source / "workspace/earlier").write_bytes(b"earlier")
    original = os.read

    def reading(fd, n):
        data = original(fd, n)
        if data == b"earlier":
            (source / "outputs/result").unlink()
            (source / "outputs/result").write_bytes(b"content")
        return data

    monkeypatch.setattr(os, "read", reading)
    with pytest.raises(ValueError, match="changed|replacement"):
        seal(store, source)


def deep_zero_byte_files(source, count):
    """Build valid long paths through descriptors, beyond macOS Path limits."""
    fd = os.open(source / "workspace", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for level in range(10):
            component = str(level) + "d" * 179
            os.mkdir(component, mode=0o700, dir_fd=fd)
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        for index in range(count):
            leaf = os.open(f"file-{index:04d}", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
            os.close(leaf)
    finally:
        os.close(fd)


def test_oversized_real_metadata_rejected_before_publication_and_marker(storage):
    store, source = storage
    deep_zero_byte_files(source, 3000)
    fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        dirs, files, total = store._inventory(fd)
        # All paths/entries/data bytes satisfy their independent bounds. The
        # serialized inventory exceeds the actual fixed 2MiB metadata budget.
        assert len(dirs) + len(files) < 4096 and total == 7
        inventory = api().canonical(dict(directories=dirs, files=[f.model_dump() for f in files]))
        assert len(inventory) > api().MAX_METADATA_BYTES
        with pytest.raises(ValueError):
            store.seal(identity(), fd)
    finally:
        os.close(fd)
    parts = (".fleet-agent-workspaces", "user", "thread", "task", "attempt")
    with store.nas.directory(parts) as parent:
        assert os.listdir(parent) == [], "Rejected metadata must leave no version, request marker, or temporary candidate"


def test_real_metadata_near_fixed_budget_roundtrips(storage, tmp_path):
    store, source = storage
    deep_zero_byte_files(source, 1000)
    point = seal(store, source)
    serialized = api().canonical(point.model_dump(mode="json"))
    assert api().MAX_METADATA_BYTES * 0.85 < len(serialized) <= api().MAX_METADATA_BYTES
    assert store.verify(point) == point
    destination = tmp_path / "near-budget-restore"
    destination.mkdir()
    fd = os.open(destination, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        store.restore(point, fd)
        store.restore(point, fd)
    finally:
        os.close(fd)
