"""Bounded artifact metadata and descriptor-based copies of stopped job outputs."""

import hashlib
import os
import stat
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_ENTRIES = 4096
MANIFEST_NAME = ".fleet-manifest.json"
OPEN_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def relative_parts(value):
    if not isinstance(value, str) or not value or len(value.encode()) > 2048:
        raise ValueError("Invalid artifact path")
    parts = value.split("/")
    if len(parts) > 64:
        raise ValueError("Artifact path exceeds depth limit")
    if any(part in {"", ".", ".."} or "\\" in part or "\0" in part or len(part.encode()) > 255 for part in parts):
        raise ValueError("Invalid artifact path")
    return parts


class ArtifactFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    path: str
    size: int = Field(ge=0, strict=True)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @field_validator("path")
    @classmethod
    def valid_path(cls, value):
        relative_parts(value)
        if MANIFEST_NAME in value.split("/"):
            raise ValueError("Reserved manifest path")
        return value


class SealedManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal[1] = 1
    output_prefix: str
    files: list[ArtifactFile] = Field(max_length=MAX_ENTRIES)
    total_bytes: int = Field(ge=0, strict=True)

    @model_validator(mode="after")
    def consistent(self):
        paths = [row.path for row in self.files]
        if paths != sorted(set(paths)) or self.total_bytes != sum(row.size for row in self.files):
            raise ValueError("Inconsistent artifact manifest")
        return self


def copy_outputs(source_fd, destination_fd, *, max_bytes):
    if max_bytes <= 0:
        raise ValueError("Invalid output limit")
    rows = []
    total = count = 0

    def walk(source, destination, prefix):
        nonlocal total, count
        names = []
        with os.scandir(source) as entries:
            for entry in entries:
                count += 1
                if count > MAX_ENTRIES:
                    raise ValueError("Output entry limit exceeded")
                relative_parts(entry.name)
                if entry.name == MANIFEST_NAME:
                    raise ValueError("Reserved manifest path")
                names.append(entry.name)
        for name in sorted(names):
            path = "/".join([*prefix, name])
            relative_parts(path)
            info = os.stat(name, dir_fd=source, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                child = os.open(name, OPEN_DIRECTORY, dir_fd=source)
                try:
                    os.mkdir(name, mode=0o700, dir_fd=destination)
                    target = os.open(name, OPEN_DIRECTORY, dir_fd=destination)
                    try:
                        walk(child, target, [*prefix, name])
                    finally:
                        os.close(target)
                finally:
                    os.close(child)
            elif stat.S_ISREG(info.st_mode):
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=source)
                try:
                    before = os.fstat(fd)
                    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or total + before.st_size > max_bytes:
                        raise ValueError("Unsafe or oversized output file")
                    target = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=destination)
                    try:
                        digest = hashlib.sha256()
                        size = 0
                        while chunk := os.read(fd, min(65536, max_bytes - total + 1)):
                            size += len(chunk)
                            total += len(chunk)
                            if total > max_bytes:
                                raise ValueError("Output byte limit exceeded")
                            digest.update(chunk)
                            view = memoryview(chunk)
                            while view:
                                written = os.write(target, view)
                                view = view[written:]
                        after = os.fstat(fd)
                        if size != before.st_size or (before.st_size, before.st_mtime_ns, before.st_ctime_ns, before.st_nlink) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns, after.st_nlink):
                            raise ValueError("Output changed during sealing")
                        os.fchmod(target, 0o400)
                        os.fsync(target)
                        rows.append({"path": path, "size": size, "sha256": digest.hexdigest()})
                    finally:
                        os.close(target)
                finally:
                    os.close(fd)
            else:
                raise ValueError("Only regular files and directories may be sealed")
        os.fsync(destination)

    walk(source_fd, destination_fd, [])
    return sorted(rows, key=lambda row: row["path"]), total


def open_verified_file(directory_fd, metadata):
    parts = relative_parts(metadata.path)
    parent = os.dup(directory_fd)
    fd = None
    try:
        for part in parts[:-1]:
            child = os.open(part, OPEN_DIRECTORY, dir_fd=parent)
            os.close(parent)
            parent = child
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size != metadata.size:
            raise ValueError("Sealed artifact metadata mismatch")
        digest = hashlib.sha256()
        size = 0
        while chunk := os.read(fd, min(65536, metadata.size - size + 1)):
            size += len(chunk)
            if size > metadata.size:
                raise ValueError("Sealed artifact grew during validation")
            digest.update(chunk)
        if size != metadata.size or digest.hexdigest() != metadata.sha256:
            raise ValueError("Sealed artifact digest mismatch")
        os.lseek(fd, 0, os.SEEK_SET)
        file = os.fdopen(fd, "rb")
        fd = None
        return file
    finally:
        os.close(parent)
        if fd is not None:
            os.close(fd)
