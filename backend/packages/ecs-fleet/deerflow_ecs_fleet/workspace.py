"""NAS sentinel and attempt-scoped workspaces; run filesystem calls in a thread."""

import os
import re
import shutil
import stat
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from .artifacts import MANIFEST_NAME, OPEN_DIRECTORY, SealedManifest, copy_outputs, open_verified_file, relative_parts
from .config import NAME_PATTERN


class NASWorkspace:
    def __init__(self, root: Path, *, identity: str):
        self.root = Path(root)
        if not self.root.is_absolute() or ".." in self.root.parts or not re.fullmatch(NAME_PATTERN, identity):
            raise ValueError("Absolute NAS root and explicit deployment identity required")
        self.identity = identity

    @contextmanager
    def directory(self, parts=(), *, create=False):
        # Open every component without following symlinks, including the root.
        fd = os.open("/", OPEN_DIRECTORY)
        try:
            for component in self.root.parts[1:]:
                child = os.open(component, OPEN_DIRECTORY, dir_fd=fd)
                os.close(fd)
                fd = child
            sentinel = os.open(".deerflow-fleet-root", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            try:
                info = os.fstat(sentinel)
                if not stat.S_ISREG(info.st_mode) or info.st_size > 256 or info.st_nlink != 1:
                    raise ValueError("Unsafe NAS sentinel")
                if os.read(sentinel, 257) != (self.identity + "\n").encode():
                    raise ValueError("NAS deployment identity mismatch")
            finally:
                os.close(sentinel)
            for component in parts:
                relative_parts(component)
                if "/" in component:
                    raise ValueError("Invalid directory component")
                if create:
                    try:
                        os.mkdir(component, mode=0o700, dir_fd=fd)
                    except FileExistsError:
                        pass
                child = os.open(component, OPEN_DIRECTORY, dir_fd=fd)
                os.close(fd)
                fd = child
            yield fd
        finally:
            os.close(fd)

    @staticmethod
    def attempt_parts(claim, grant):
        spec = grant["launch_spec"]
        expected = [spec["user_id"], spec["thread_id"], "jobs", claim["job_id"], "attempts", claim["attempt_id"]]
        if any(not isinstance(value, str) or re.fullmatch(NAME_PATTERN, value) is None for value in expected):
            raise ValueError("Invalid workspace identity")
        if grant.get("authorized") is not True or grant["attempt_id"] != claim["attempt_id"] or grant["job_id"] != claim["job_id"] or grant["output_prefix"] != claim["output_prefix"] or claim["output_prefix"] != "/".join(expected):
            raise ValueError("Workspace identity mismatch")
        return expected

    def prepare(self, claim, grant):
        parts = self.attempt_parts(claim, grant)
        with self.directory(parts, create=True) as fd:
            try:
                os.mkdir("outputs", mode=0o777, dir_fd=fd)
            except FileExistsError:
                pass
            output = os.open("outputs", OPEN_DIRECTORY, dir_fd=fd)
            try:
                os.fchmod(output, 0o777)
            finally:
                os.close(output)
        return self.root.joinpath(*parts, "outputs")

    def seal(self, claim, grant, *, stopped: bool, max_bytes: int):
        if stopped is not True:
            raise ValueError("Physical stop proof required before sealing")
        parts = self.attempt_parts(claim, grant)
        with self.directory(parts) as parent:
            try:
                existing = os.open("sealed", OPEN_DIRECTORY, dir_fd=parent)
            except FileNotFoundError:
                existing = None
            if existing is not None:
                try:
                    fd = os.open(MANIFEST_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=existing)
                    with os.fdopen(fd, "rb") as file:
                        info = os.fstat(file.fileno())
                        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                            raise ValueError("Unsafe sealed manifest file")
                        data = file.read(2 * 1024 * 1024 + 1)
                    if len(data) > 2 * 1024 * 1024:
                        raise ValueError("Manifest exceeds limit")
                    manifest = SealedManifest.model_validate_json(data)
                    if manifest.output_prefix != claim["output_prefix"] + "/sealed" or manifest.total_bytes > max_bytes:
                        raise ValueError("Sealed manifest identity or size mismatch")
                    for row in manifest.files:
                        with open_verified_file(existing, row):
                            pass
                    return manifest.model_dump()
                finally:
                    os.close(existing)
            staging_name = ".seal-" + uuid4().hex
            os.mkdir(staging_name, mode=0o700, dir_fd=parent)
            staging = os.open(staging_name, OPEN_DIRECTORY, dir_fd=parent)
            source = None
            published = False
            try:
                source = os.open("outputs", OPEN_DIRECTORY, dir_fd=parent)
                files, total = copy_outputs(source, staging, max_bytes=max_bytes)
                manifest = SealedManifest(output_prefix=claim["output_prefix"] + "/sealed", files=files, total_bytes=total)
                fd = os.open(MANIFEST_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400, dir_fd=staging)
                with os.fdopen(fd, "wb") as file:
                    file.write(manifest.model_dump_json().encode())
                    file.flush()
                    os.fsync(file.fileno())
                self.directory_modes(staging, 0o500)
                os.fsync(staging)
                # The new tree is never mounted into the old task container.
                # Rename publishes the complete snapshot, not its writable source.
                os.rename(staging_name, "sealed", src_dir_fd=parent, dst_dir_fd=parent)
                published = True
                os.fsync(parent)
                return manifest.model_dump()
            finally:
                if source is not None:
                    os.close(source)
                try:
                    if not published:
                        self.directory_modes(staging, 0o700)
                        shutil.rmtree(staging_name, dir_fd=parent)
                finally:
                    os.close(staging)

    @staticmethod
    def directory_modes(fd, mode):
        os.fchmod(fd, mode)
        with os.scandir(fd) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    child = os.open(entry.name, OPEN_DIRECTORY, dir_fd=fd)
                    try:
                        NASWorkspace.directory_modes(child, mode)
                    finally:
                        os.close(child)
        os.fsync(fd)

    def open_artifact(self, manifest, relative_path):
        manifest = SealedManifest.model_validate(manifest)
        parts = relative_parts(manifest.output_prefix)
        if len(parts) != 7 or parts[2] != "jobs" or parts[4] != "attempts" or parts[6] != "sealed" or any(re.fullmatch(NAME_PATTERN, part) is None for part in parts):
            raise ValueError("Invalid sealed prefix")
        relative_parts(relative_path)
        metadata = next((row for row in manifest.files if row.path == relative_path), None)
        if metadata is None:
            raise ValueError("Artifact is not in accepted manifest")
        with self.directory(parts) as fd:
            return open_verified_file(fd, metadata)
