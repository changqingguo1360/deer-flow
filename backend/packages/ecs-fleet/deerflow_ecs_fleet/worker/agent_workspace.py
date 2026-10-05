"""Trusted first-launch Agent inputs, copied from immutable NAS snapshots."""

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ..artifacts import OPEN_DIRECTORY, relative_parts
from ..config import NAME_PATTERN

MAX_MANIFEST_BYTES = 1024 * 1024
MAX_FILES = 4096
MAX_INPUT_BYTES = 2**31 - 1
MAX_FILE_BYTES = 64 * 1024 * 1024


class AgentWorkspaceFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    category: Literal["workspace", "uploads"]
    path: str
    size: int = Field(ge=0, le=MAX_FILE_BYTES, strict=True)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @field_validator("path")
    @classmethod
    def safe_path(cls, value):
        relative_parts(value)
        return value


class AgentWorkspaceManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1] = 1
    user_id: str
    thread_id: str
    files: tuple[AgentWorkspaceFile, ...] = Field(max_length=MAX_FILES)
    total_bytes: int = Field(ge=0, le=MAX_INPUT_BYTES, strict=True)

    @field_validator("user_id", "thread_id")
    @classmethod
    def safe_identity(cls, value):
        if not re.fullmatch(NAME_PATTERN, value):
            raise ValueError("Invalid workspace owner identity")
        return value

    @model_validator(mode="after")
    def consistent(self):
        keys = [(item.category, item.path) for item in self.files]
        if keys != sorted(set(keys)) or self.total_bytes != sum(item.size for item in self.files):
            raise ValueError("Inconsistent Agent workspace manifest")
        return self

    def canonical_bytes(self):
        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()

    @property
    def reference(self):
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


class AgentWorkspaceSnapshots:
    def __init__(self, nas_root, *, state_dir, max_input_bytes=64 * 1024 * 1024):
        self.root = Path(nas_root)
        self.state_dir = Path(state_dir)
        if self.state_dir.resolve().is_relative_to(self.root.resolve()):
            raise ValueError("Prepared workspace control state must remain outside NAS")
        if type(max_input_bytes) is not int or max_input_bytes <= 0:
            raise ValueError("Invalid operator workspace input budget")
        self.max_input_bytes = max_input_bytes

    @staticmethod
    def open_path(root_fd, parts, flags):
        current = os.dup(root_fd)
        try:
            for part in parts[:-1]:
                child = os.open(part, OPEN_DIRECTORY, dir_fd=current)
                os.close(current)
                current = child
            return os.open(parts[-1], flags | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=current)
        finally:
            os.close(current)

    @staticmethod
    def create_file(root_fd, parts):
        current = os.dup(root_fd)
        try:
            for part in parts[:-1]:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=current)
                    os.fsync(current)
                except FileExistsError:
                    pass
                child = os.open(part, OPEN_DIRECTORY, dir_fd=current)
                os.close(current)
                current = child
            fd = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=current)
            os.fsync(current)
            return fd
        finally:
            os.close(current)

    @staticmethod
    def prepare_directory(root_fd, parts, *, prepared):
        current = os.dup(root_fd)
        try:
            for part in parts:
                if not prepared:
                    try:
                        os.mkdir(part, mode=0o700, dir_fd=current)
                        os.fsync(current)
                    except FileExistsError:
                        pass
                child = os.open(part, OPEN_DIRECTORY, dir_fd=current)
                os.close(current)
                current = child
        finally:
            os.close(current)

    def prepare(self, spec, attempt_root):
        ref = spec["workspace_manifest_ref"]
        user, thread = spec["user_id"], spec["thread_id"]
        if not re.fullmatch(r"[a-f0-9]{64}", ref) or not all(re.fullmatch(NAME_PATTERN, item) for item in (user, thread)):
            raise ValueError("Invalid Agent workspace manifest reference")
        identity = {"reference": ref, "user_id": user, "thread_id": thread}
        target = Path(attempt_root)
        from .journal import AttemptJournal

        AttemptJournal(self.state_dir).directory()
        identity["attempt_root"] = str(target.resolve())
        marker = self.state_dir / (hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest() + ".json")
        prepared = marker.exists()
        if prepared:
            fd = os.open(marker, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as file:
                info = os.fstat(file.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_MANIFEST_BYTES or info.st_mode & 0o077 or info.st_uid != os.getuid():
                    raise ValueError("Unsafe prepared workspace control state")
                if json.loads(file.read(MAX_MANIFEST_BYTES + 1)) != identity:
                    raise ValueError("Prepared Agent workspace identity changed")
        root_fd = os.open(self.root, OPEN_DIRECTORY)
        destination_fd = None
        try:
            destination_fd = os.open(target, OPEN_DIRECTORY)
            prefix = [".fleet-agent-inputs", user, thread, ref]
            try:
                fd = self.open_path(root_fd, [*prefix, "manifest.json"], os.O_RDONLY)
                with os.fdopen(fd, "rb") as file:
                    info = os.fstat(file.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_MANIFEST_BYTES:
                        raise ValueError("Unsafe Agent workspace manifest")
                    manifest = AgentWorkspaceManifest.model_validate_json(file.read(MAX_MANIFEST_BYTES + 1))
            except OSError:
                raise ValueError("Agent workspace manifest is missing or unsafe") from None
            if manifest.total_bytes > self.max_input_bytes:
                raise ValueError("Agent workspace exceeds operator input budget")
            if manifest.reference != ref or (manifest.user_id, manifest.thread_id) != (user, thread):
                raise ValueError("Agent workspace manifest ownership or content changed")
            data_parts = [".deer-flow", "users", user, "threads", thread, "user-data"]
            for item in manifest.files:
                try:
                    source_fd = self.open_path(root_fd, [*prefix, item.category, *relative_parts(item.path)], os.O_RDONLY)
                except OSError:
                    raise ValueError("Agent workspace source is missing or unsafe") from None
                with os.fdopen(source_fd, "rb") as source:
                    before = os.fstat(source.fileno())
                    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size != item.size:
                        raise ValueError("Unsafe Agent workspace source")
                    digest = hashlib.sha256()
                    copied = 0
                    out_fd = os.open(os.devnull, os.O_WRONLY) if prepared else self.create_file(destination_fd, [*data_parts, item.category, *relative_parts(item.path)])
                    with os.fdopen(out_fd, "wb") as output:
                        while chunk := source.read(65536):
                            copied += len(chunk)
                            if copied > item.size:
                                raise ValueError("Agent workspace file changed during copy")
                            digest.update(chunk)
                            output.write(chunk)
                        output.flush()
                        if not prepared:
                            os.fsync(output.fileno())
                    after = os.fstat(source.fileno())
                    if copied != item.size or digest.hexdigest() != item.sha256 or (before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_nlink) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_nlink):
                        raise ValueError("Agent workspace content changed")
            # No sandbox/tool is required for a legal first execution. Create
            # only the original attempt's approved roots after all input files
            # were verified. A prepared retry validates without recreating them.
            for category in ("workspace", "uploads", "outputs"):
                self.prepare_directory(destination_fd, [*data_parts, category], prepared=prepared)
            if prepared:
                return
            fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as file:
                file.write(json.dumps(identity, sort_keys=True).encode())
                file.flush()
                os.fsync(file.fileno())
            os.fsync(destination_fd)
            control_fd = os.open(self.state_dir, OPEN_DIRECTORY)
            try:
                os.fsync(control_fd)
            finally:
                os.close(control_fd)
        finally:
            if destination_fd is not None:
                os.close(destination_fd)
            os.close(root_fd)

    def prepare_accepted(self, spec, attempt_root, accepted, *, nas_identity, max_output_bytes):
        """Clone an original accepted point into owned user-data on every retry."""
        from ..agent_workspace import AgentWorkspaceVersions, WorkspaceManifest, canonical
        from ..workspace import NASWorkspace
        from .journal import AttemptJournal

        manifest = WorkspaceManifest.model_validate(accepted["manifest"])
        if (manifest.user_id, manifest.thread_id) != (spec["user_id"], spec.get("source_workspace_thread_id") or spec["thread_id"]):
            raise ValueError("Accepted workspace owner conflicts")
        if not all(isinstance(accepted.get(key), str) and accepted[key] for key in ("point_id", "checkpoint_id")):
            raise ValueError("Original accepted workspace point required")
        if spec.get("source_workspace_point_id") not in (None, accepted["point_id"]):
            raise ValueError("Accepted point conflicts with frozen launch")
        if spec.get("source_workspace_checkpoint_id") not in (None, accepted["checkpoint_id"]):
            raise ValueError("Accepted source checkpoint conflicts with frozen launch")
        target = Path(attempt_root)
        if target.resolve() == (self.root / manifest.nas_prefix).resolve() or target.resolve().is_relative_to((self.root / manifest.nas_prefix).resolve()):
            raise ValueError("Accepted workspace cannot alias an execution attempt")
        AttemptJournal(self.state_dir).directory()
        identity = dict(
            attempt_root=str(target.resolve()),
            user_id=spec["user_id"],
            thread_id=spec["thread_id"],
            source_thread_id=manifest.thread_id,
            point_id=accepted["point_id"],
            checkpoint_id=accepted["checkpoint_id"],
            manifest_id=manifest.manifest_id,
        )
        marker = self.state_dir / (hashlib.sha256(canonical([str(target.resolve()), "accepted"])).hexdigest() + ".json")
        if marker.exists():
            fd = os.open(marker, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as file:
                info = os.fstat(file.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_MANIFEST_BYTES or info.st_mode & 0o077 or info.st_uid != os.getuid():
                    raise ValueError("Unsafe accepted workspace control state")
                if file.read(MAX_MANIFEST_BYTES + 1) != canonical(identity):
                    raise ValueError("Original accepted workspace selection changed")
        versions = AgentWorkspaceVersions(NASWorkspace(self.root, identity=nas_identity), max_input_bytes=self.max_input_bytes, max_output_bytes=max_output_bytes)
        versions.verify(manifest)
        fd = os.open(target, OPEN_DIRECTORY)
        try:
            for part in (".deer-flow", "users", spec["user_id"], "threads", spec["thread_id"], "user-data"):
                try:
                    os.mkdir(part, mode=0o700, dir_fd=fd)
                    os.fsync(fd)
                except FileExistsError:
                    pass
                child = os.open(part, OPEN_DIRECTORY, dir_fd=fd)
                os.close(fd)
                fd = child
            versions.restore(manifest, fd)
            os.fsync(fd)
        finally:
            os.close(fd)
        if not marker.exists():
            fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as file:
                file.write(canonical(identity))
                file.flush()
                os.fsync(file.fileno())
            fd = os.open(self.state_dir, OPEN_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
