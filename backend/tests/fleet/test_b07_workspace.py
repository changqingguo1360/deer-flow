"""Actual NAS-like directories prove isolation, immutable sealing and no-follow reads."""

import importlib
import importlib.util
import os
from uuid import uuid4

import pytest


def workspace_class():
    assert importlib.util.find_spec("deerflow_ecs_fleet.workspace") is not None, "Fleet NAS workspace missing"
    return importlib.import_module("deerflow_ecs_fleet.workspace").NASWorkspace


def nas(tmp_path):
    root = tmp_path / "nas"
    root.mkdir()
    identity = str(uuid4())
    (root / ".deerflow-fleet-root").write_text(identity + "\n")
    return workspace_class()(root, identity=identity), root


def identity(job="job", attempt="attempt"):
    prefix = f"u/t/jobs/{job}/attempts/{attempt}"
    claim = {"job_id": job, "attempt_id": attempt, "output_prefix": prefix}
    grant = {"authorized": True, "attempt_id": attempt, "job_id": job, "output_prefix": prefix, "launch_spec": {"user_id": "u", "thread_id": "t"}}
    return claim, grant


@pytest.mark.parametrize("fault", ["missing_root", "missing_sentinel", "wrong_sentinel", "symlink_sentinel"])
def test_missing_nas_never_creates_local_fallback(tmp_path, fault):
    cls = workspace_class()
    root = tmp_path / "nas"
    if fault != "missing_root":
        root.mkdir()
    if fault == "wrong_sentinel":
        (root / ".deerflow-fleet-root").write_text("another-deployment\n")
    if fault == "symlink_sentinel":
        outside = tmp_path / "sentinel"
        outside.write_text("deployment\n")
        (root / ".deerflow-fleet-root").symlink_to(outside)
    with pytest.raises((ValueError, OSError)):
        cls(root, identity="deployment").prepare(*identity())
    assert fault != "missing_root" or not root.exists()
    assert not (root / "u").exists()


@pytest.mark.parametrize("fault", ["escape_prefix", "foreign_owner", "foreign_attempt", "foreign_job", "symlink_parent"])
def test_workspace_rejects_escape_and_identity_mismatch(tmp_path, fault):
    ws, root = nas(tmp_path)
    claim, grant = identity()
    if fault == "escape_prefix":
        claim["output_prefix"] = "../outside"
    elif fault == "foreign_owner":
        grant["launch_spec"]["user_id"] = "other"
    elif fault == "foreign_attempt":
        grant["attempt_id"] = "other"
    elif fault == "foreign_job":
        claim["job_id"] = "other"
        claim["output_prefix"] = "u/t/jobs/other/attempts/attempt"
    else:
        outside = tmp_path / "outside"
        outside.mkdir()
        (root / "u").symlink_to(outside, target_is_directory=True)
    with pytest.raises((ValueError, OSError)):
        ws.prepare(claim, grant)
    if fault == "symlink_parent":
        assert list((tmp_path / "outside").iterdir()) == []


def test_sealed_outputs_are_independent_of_old_writer_and_reads_keep_fd(tmp_path):
    ws, root = nas(tmp_path)
    claim, grant = identity()
    output = ws.prepare(claim, grant)
    (output / "folder").mkdir()
    (output / "folder" / "result.txt").write_text("finished")
    other = ws.prepare(*identity("other-job", "other-attempt"))
    assert other != output and list(other.iterdir()) == []
    with pytest.raises(ValueError, match="stop"):
        ws.seal(claim, grant, stopped=False, max_bytes=1024)
    manifest = ws.seal(claim, grant, stopped=True, max_bytes=1024)
    assert manifest["total_bytes"] == 8
    assert [row["path"] for row in manifest["files"]] == ["folder/result.txt"]
    (output / "folder" / "result.txt").write_text("old writer changed this")
    assert ws.seal(claim, grant, stopped=True, max_bytes=1024) == manifest
    with ws.open_artifact(manifest, "folder/result.txt") as file:
        sealed = root / manifest["output_prefix"] / "folder" / "result.txt"
        sealed.parent.chmod(0o700)
        sealed.rename(sealed.with_name("original.txt"))
        sealed.symlink_to(output / "folder" / "result.txt")
        assert file.read() == b"finished"
    with pytest.raises((ValueError, OSError)):
        ws.open_artifact(manifest, "folder/result.txt")
    with pytest.raises(ValueError):
        ws.open_artifact(manifest, "../outside")


@pytest.mark.parametrize("fault", ["symlink_file", "symlink_directory", "fifo", "hardlink", "oversized"])
def test_unsafe_outputs_cannot_be_sealed(tmp_path, fault):
    ws, root = nas(tmp_path)
    claim, grant = identity()
    output = ws.prepare(claim, grant)
    outside = tmp_path / "outside"
    outside.write_text("secret")
    if fault == "symlink_file":
        (output / "file").symlink_to(outside)
    elif fault == "symlink_directory":
        (output / "dir").symlink_to(tmp_path, target_is_directory=True)
    elif fault == "fifo":
        os.mkfifo(output / "pipe")
    elif fault == "hardlink":
        os.link(outside, output / "file")
    else:
        (output / "big").write_bytes(b"x" * 65)
    with pytest.raises((ValueError, OSError)):
        ws.seal(claim, grant, stopped=True, max_bytes=64)
    assert not (root / claim["output_prefix"] / "sealed").exists()
    assert outside.read_text() == "secret"


def test_nas_disappearance_after_prepare_blocks_seal_without_fallback(tmp_path):
    ws, root = nas(tmp_path)
    claim, grant = identity()
    output = ws.prepare(claim, grant)
    (output / "result").write_text("finished")
    (root / ".deerflow-fleet-root").unlink()
    with pytest.raises((ValueError, OSError)):
        ws.seal(claim, grant, stopped=True, max_bytes=1024)
    assert not (root / claim["output_prefix"] / "sealed").exists()
