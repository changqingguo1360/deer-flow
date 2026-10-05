"""Actual fixed-marker candidate recovery; the B journal remains bounded."""

import hashlib
import json
from dataclasses import replace

import pytest

from .test_c08_workspace import identity, seal  # noqa: F401
from .test_c08_workspace import storage as storage


def marker(store, boundary):
    from deerflow_ecs_fleet.agent_workspace import canonical

    name = ".request-" + hashlib.sha256(canonical([boundary.attempt_id, boundary.checkpoint_id, boundary.kind, boundary.publication_key])).hexdigest()
    return store.nas.root / ".fleet-agent-workspaces" / boundary.user_id / boundary.thread_id / boundary.agent_task_id / boundary.attempt_id / name


def test_recovery_is_pure_read_and_missing_marker_only_means_none(storage):  # noqa: F811
    store, source = storage
    boundary = identity()
    assert getattr(store, "recover", None) is not None, "Trusted fixed-marker recovery is absent"
    assert store.recover(boundary) is None
    assert not (store.nas.root / ".fleet-agent-workspaces").exists()
    candidate = seal(store, source)
    assert store.recover(boundary) == candidate
    assert store.recover(replace(boundary, checkpoint_id="new-checkpoint")) is None


@pytest.mark.parametrize("change", ("identity", "digest", "missing_file", "marker_symlink", "manifest_symlink"))
def test_recovery_rejects_foreign_or_unsafe_actual_candidate(storage, change):  # noqa: F811
    store, source = storage
    boundary = identity()
    candidate = seal(store, source)
    assert getattr(store, "recover", None) is not None, "Trusted fixed-marker recovery is absent"
    selected = marker(store, boundary)
    if change == "identity":
        boundary = replace(boundary, token_stamp="f" * 64)
    elif change == "digest":
        value = json.loads(selected.read_text())
        value["request_digest"] = "f" * 64
        selected.write_text(json.dumps(value))
    elif change == "missing_file":
        (store.nas.root / candidate.nas_prefix / "outputs/result").unlink()
    else:
        selected = selected if change == "marker_symlink" else store.nas.root / candidate.nas_prefix / "manifest.json"
        real = selected.with_name(selected.name + ".real")
        selected.rename(real)
        selected.symlink_to(real)
    with pytest.raises((ValueError, OSError)):
        store.recover(boundary)


def test_near_two_mib_candidate_reloads_with_only_bounded_journal_pointer(storage, tmp_path):  # noqa: F811
    from deerflow_ecs_fleet.agent_workspace import MAX_METADATA_BYTES, canonical
    from deerflow_ecs_fleet.worker.journal import AttemptJournal

    from .test_c08_workspace import deep_zero_byte_files

    store, source = storage
    deep_zero_byte_files(source, 1000)
    boundary = identity()
    candidate = seal(store, source)
    assert MAX_METADATA_BYTES * 0.85 < len(canonical(candidate.model_dump(mode="json"))) <= MAX_METADATA_BYTES
    journal = AttemptJournal(tmp_path / "private-journal")
    pointer = {"request_id": boundary.request_id, "request_digest": boundary.request_digest, "barrier_epoch": 1, "nonce": "e" * 64, "manifest_id": candidate.manifest_id}
    journal.save({"claim": {"attempt_id": boundary.attempt_id}, "workspace_candidate": pointer})
    reloaded = journal.records()[0]["workspace_candidate"]
    assert reloaded == pointer
    verified = store.recover(boundary)
    assert verified == candidate and verified.manifest_id == reloaded["manifest_id"]


def test_actual_marker_deleted_after_descriptor_read_is_rejected_not_missing(storage, monkeypatch):  # noqa: F811
    import os

    store, source = storage
    boundary = identity()
    seal(store, source)
    selected = marker(store, boundary)
    observed = selected.stat()
    original_read, original_stat = os.read, os.stat
    read_completed, deleted = [], []

    def observed_read(fd, count):
        data = original_read(fd, count)
        info = os.fstat(fd)
        if (info.st_dev, info.st_ino) == (observed.st_dev, observed.st_ino):
            read_completed.append(True)
        return data

    def delete_before_named_recheck(path, *args, **kwargs):
        # Real unlink after the descriptor's post-read fstat, immediately before
        # the final named-entry check. That stat must fail, never mean absent.
        if read_completed and not deleted and path == selected.name:
            selected.unlink()
            deleted.append(True)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "read", observed_read)
    monkeypatch.setattr(os, "stat", delete_before_named_recheck)
    with pytest.raises((ValueError, OSError)):
        store.recover(boundary)
    assert deleted == [True]
