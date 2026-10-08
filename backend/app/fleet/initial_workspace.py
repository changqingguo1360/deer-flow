"""Capture authenticated host inputs into the worker's immutable NAS contract."""

import hashlib
import os
import shutil
import stat
from uuid import uuid4

from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions, bounded_names, no_replace, signature
from deerflow_ecs_fleet.artifacts import OPEN_DIRECTORY, relative_parts
from deerflow_ecs_fleet.worker.agent_workspace import MAX_FILE_BYTES, MAX_FILES, MAX_MANIFEST_BYTES, AgentWorkspaceFile, AgentWorkspaceManifest
from deerflow_ecs_fleet.workspace import NASWorkspace

from deerflow.config.paths import get_paths


def capture(config, *, user_id, thread_id):
    owner = AgentWorkspaceManifest(user_id=user_id, thread_id=thread_id, files=(), total_bytes=0)
    nas = NASWorkspace(config.nas_root, identity=config.nas_identity)
    versions = AgentWorkspaceVersions(nas, max_input_bytes=config.max_input_bytes, max_output_bytes=config.max_input_bytes)
    source = get_paths().sandbox_user_data_dir(thread_id, user_id=user_id)
    rows, observations, total, count = [], [], 0, 0
    with versions._parent((".fleet-agent-inputs", user_id, thread_id)) as parent:
        stage = ".capture-" + uuid4().hex
        os.mkdir(stage, mode=0o700, dir_fd=parent)
        target = source_fd = None
        try:
            target = os.open(stage, OPEN_DIRECTORY, dir_fd=parent)
            try:
                # Every ancestor is descriptor-opened without following links.
                source_fd = os.open(source.anchor, OPEN_DIRECTORY)
                for component in source.parts[1:]:
                    child = os.open(component, OPEN_DIRECTORY, dir_fd=source_fd)
                    os.close(source_fd)
                    source_fd = child
            except FileNotFoundError:
                if source_fd is not None:
                    os.close(source_fd)
                source_fd = None

            def walk(fd, out, category, prefix):
                nonlocal total, count
                before = signature(os.fstat(fd))
                names = bounded_names(fd, MAX_FILES - count)
                for name in names:
                    if category == "uploads" and name.startswith(".upload-") and name.endswith(".part"):
                        continue
                    parts = relative_parts("/".join((*prefix, name)))
                    count += 1
                    if count > MAX_FILES:
                        raise ValueError("Initial workspace entry limit exceeded")
                    info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                    observations.append((category, parts, signature(info)))
                    if stat.S_ISDIR(info.st_mode):
                        child = os.open(name, OPEN_DIRECTORY, dir_fd=fd)
                        try:
                            os.mkdir(name, mode=0o700, dir_fd=out)
                            dest = os.open(name, OPEN_DIRECTORY, dir_fd=out)
                            try:
                                if signature(os.fstat(child)) != signature(info):
                                    raise ValueError("Initial workspace directory changed")
                                walk(child, dest, category, parts)
                            finally:
                                os.close(dest)
                        finally:
                            os.close(child)
                    elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                        if info.st_size > MAX_FILE_BYTES or total + info.st_size > config.max_input_bytes:
                            raise ValueError("Initial workspace byte limit exceeded")
                        leaf = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                        try:
                            dest = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=out)
                            try:
                                digest, size = hashlib.sha256(), 0
                                if signature(os.fstat(leaf)) != signature(info):
                                    raise ValueError("Initial workspace file changed")
                                while chunk := os.read(leaf, 65536):
                                    size += len(chunk)
                                    if size > info.st_size:
                                        raise ValueError("Initial workspace file grew")
                                    digest.update(chunk)
                                    view = memoryview(chunk)
                                    while view:
                                        view = view[os.write(dest, view) :]
                                if size != info.st_size or signature(os.fstat(leaf)) != signature(info):
                                    raise ValueError("Initial workspace file changed")
                                os.fsync(dest)
                                total += size
                                rows.append(AgentWorkspaceFile(category=category, path="/".join(parts), size=size, sha256=digest.hexdigest()))
                            finally:
                                os.close(dest)
                        finally:
                            os.close(leaf)
                    else:
                        raise ValueError("Initial workspace requires regular files without links")
                    if signature(os.stat(name, dir_fd=fd, follow_symlinks=False)) != signature(info):
                        raise ValueError("Initial workspace source changed")
                if bounded_names(fd, len(names)) != names or signature(os.fstat(fd)) != before:
                    raise ValueError("Initial workspace directory changed")
                os.fsync(out)

            roots = []
            for category in ("uploads", "workspace"):
                os.mkdir(category, mode=0o700, dir_fd=target)
                if source_fd is None:
                    continue
                try:
                    fd = os.open(category, OPEN_DIRECTORY, dir_fd=source_fd)
                except FileNotFoundError:
                    continue
                try:
                    roots.append((category, signature(os.fstat(fd))))
                    out = os.open(category, OPEN_DIRECTORY, dir_fd=target)
                    try:
                        walk(fd, out, category, ())
                    finally:
                        os.close(out)
                finally:
                    os.close(fd)
            for category, observed in roots:
                if signature(os.stat(category, dir_fd=source_fd, follow_symlinks=False)) != observed:
                    raise ValueError("Initial workspace category changed after copy")
            for category, parts, observed in observations:
                fd = os.open(category, OPEN_DIRECTORY, dir_fd=source_fd)
                try:
                    for part in parts[:-1]:
                        child = os.open(part, OPEN_DIRECTORY, dir_fd=fd)
                        os.close(fd)
                        fd = child
                    if signature(os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)) != observed:
                        raise ValueError("Initial workspace source changed after copy")
                finally:
                    os.close(fd)
            manifest = owner.model_copy(update={"files": tuple(sorted(rows, key=lambda item: (item.category, item.path))), "total_bytes": total})
            manifest = AgentWorkspaceManifest.model_validate(manifest.model_dump())
            data = manifest.canonical_bytes()
            if len(data) > MAX_MANIFEST_BYTES:
                raise ValueError("Initial workspace manifest limit exceeded")
            versions._write(target, "manifest.json", data)
            os.fsync(target)
            try:
                no_replace(stage, manifest.reference, parent)
            except FileExistsError:
                existing = os.open(manifest.reference, OPEN_DIRECTORY, dir_fd=parent)
                try:
                    if versions._read(existing, "manifest.json") != data:
                        raise ValueError("Initial workspace immutable reference conflicts")
                finally:
                    os.close(existing)
            os.fsync(parent)
            return manifest.reference
        finally:
            if source_fd is not None:
                os.close(source_fd)
            if target is not None:
                os.close(target)
            try:
                shutil.rmtree(stage, dir_fd=parent)
            except FileNotFoundError:
                pass
