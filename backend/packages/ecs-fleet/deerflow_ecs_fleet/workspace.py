"""NAS sentinel and attempt-scoped workspaces; run filesystem calls in a thread."""

import os
import re
import shutil
import stat
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from .artifacts import MANIFEST_NAME, OPEN_DIRECTORY, SealedManifest, copy_outputs, copy_uploads, open_verified_file, relative_parts
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

    @staticmethod
    def sealed_parts_valid(parts):
        shape = (len(parts) == 7 and parts[2] == "jobs" and parts[4] == "attempts" and parts[6] == "sealed") or (len(parts) == 5 and parts[2] == "inputs" and parts[4] == "sealed")
        return shape and all(re.fullmatch(NAME_PATTERN, part) is not None for part in parts)

    def publish_input(self, user_id, thread_id, identity, files, *, max_bytes):
        parts = [user_id, thread_id, "inputs", identity]
        if any(re.fullmatch(NAME_PATTERN, value) is None for value in parts):
            raise ValueError("Invalid input owner identity")
        with self.directory(parts, create=True) as parent:
            os.mkdir("sealed", mode=0o700, dir_fd=parent)
            target = os.open("sealed", OPEN_DIRECTORY, dir_fd=parent)
            published = False
            try:
                rows, total = copy_uploads(files, target, max_bytes=max_bytes)
                manifest = SealedManifest(output_prefix="/".join([*parts, "sealed"]), files=rows, total_bytes=total)
                fd = os.open(MANIFEST_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400, dir_fd=target)
                with os.fdopen(fd, "wb") as file:
                    file.write(manifest.model_dump_json().encode())
                    file.flush()
                    os.fsync(file.fileno())
                self.directory_modes(target, 0o500)
                os.fsync(parent)
                published = True
                return manifest.model_dump()
            finally:
                try:
                    if not published:
                        self.directory_modes(target, 0o700)
                        shutil.rmtree("sealed", dir_fd=parent)
                finally:
                    os.close(target)

    def validate_root(self):
        with self.directory():
            pass

    def verify_manifest(self, value):
        manifest = SealedManifest.model_validate(value)
        parts = relative_parts(manifest.output_prefix)
        if not self.sealed_parts_valid(parts):
            raise ValueError("Invalid sealed prefix")
        with self.directory(parts) as fd:
            metadata = os.open(MANIFEST_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            with os.fdopen(metadata, "rb") as file:
                info = os.fstat(file.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 2 * 1024 * 1024:
                    raise ValueError("Unsafe sealed manifest")
                stored = SealedManifest.model_validate_json(file.read(2 * 1024 * 1024 + 1))
            if stored != manifest:
                raise ValueError("Submitted manifest does not match sealed snapshot")
            for row in manifest.files:
                with open_verified_file(fd, row):
                    pass
        return manifest.model_dump()

    def prepare_inputs(self, claim, grant):
        parts = self.attempt_parts(claim, grant)
        spec = grant["launch_spec"]["spec"]
        declared = set(spec.get("input_manifests", [])) | ({spec["code_artifact_id"]} if spec.get("code_artifact_id") else set())
        snapshots = grant["launch_spec"].get("inputs", [])
        if len(snapshots) != len(declared) or {row["id"] for row in snapshots} != declared:
            raise ValueError("Input mounts do not match authorized references")
        mounts = {}
        for snapshot in snapshots:
            identity = snapshot["id"]
            if not re.fullmatch(NAME_PATTERN, identity):
                raise ValueError("Invalid input version identity")
            value = {key: snapshot[key] for key in ("schema_version", "output_prefix", "files", "total_bytes")}
            manifest = SealedManifest.model_validate(value)
            source_parts = relative_parts(manifest.output_prefix)
            if source_parts[:2] != parts[:2]:
                raise ValueError("Input snapshot owner mismatch")
            if len(source_parts) == 5 and source_parts[2] == "inputs" and source_parts[3] != identity:
                raise ValueError("Input snapshot version mismatch")
            self.verify_manifest(value)
            destination_parts = [*parts, "inputs", identity]
            with self.directory(destination_parts, create=True) as destination:
                with self.directory(source_parts) as source:
                    for row in manifest.files:
                        try:
                            existing = open_verified_file(destination, row)
                        except FileNotFoundError:
                            with open_verified_file(source, row) as file:
                                copied, total = copy_uploads([(row.path, file)], destination, max_bytes=max(1, row.size))
                            if total != row.size or copied != [row.model_dump()]:
                                raise ValueError("Input changed during materialization")
                        else:
                            existing.close()
                self.verify_input_inventory(destination, manifest)
                self.readonly_input_modes(destination)
                os.fsync(destination)
            mounts[identity] = self.root.joinpath(*destination_parts)
        return mounts

    @staticmethod
    def verify_input_inventory(fd, manifest):
        files = {row.path for row in manifest.files}
        directories = set()
        for path in files:
            parts = relative_parts(path)
            directories.update("/".join(parts[:index]) for index in range(1, len(parts)))

        def walk(directory, prefix):
            with os.scandir(directory) as entries:
                for entry in entries:
                    path = "/".join([*prefix, entry.name])
                    if entry.is_dir(follow_symlinks=False):
                        if path not in directories:
                            raise ValueError("Unlisted materialized input directory")
                        child = os.open(entry.name, OPEN_DIRECTORY, dir_fd=directory)
                        try:
                            walk(child, [*prefix, entry.name])
                        finally:
                            os.close(child)
                    elif path not in files or not entry.is_file(follow_symlinks=False):
                        raise ValueError("Unlisted or unsafe materialized input file")

        walk(fd, [])

    @staticmethod
    def readonly_input_modes(fd):
        with os.scandir(fd) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    child = os.open(entry.name, OPEN_DIRECTORY, dir_fd=fd)
                    try:
                        NASWorkspace.readonly_input_modes(child)
                    finally:
                        os.close(child)
                else:
                    leaf = os.open(entry.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                    try:
                        if not stat.S_ISREG(os.fstat(leaf).st_mode):
                            raise ValueError("Unsafe materialized input")
                        os.fchmod(leaf, 0o444)
                    finally:
                        os.close(leaf)
        os.fchmod(fd, 0o555)

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
        if not self.sealed_parts_valid(parts):
            raise ValueError("Invalid sealed prefix")
        relative_parts(relative_path)
        metadata = next((row for row in manifest.files if row.path == relative_path), None)
        if metadata is None:
            raise ValueError("Artifact is not in accepted manifest")
        with self.directory(parts) as fd:
            return open_verified_file(fd, metadata)
