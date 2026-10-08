"""Private C workspace versions. Copies run outside SQL locks and runner mounts.

Read-only modes are cosmetic: immutability is isolation plus content/inventory
verification on every use, never a same-UID chmod security claim.
"""

import ctypes
import errno
import hashlib
import json
import os
import re
import shutil
import stat
import sys
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .artifacts import MAX_ENTRIES, OPEN_DIRECTORY, relative_parts
from .config import NAME_PATTERN

CATEGORIES = ("workspace", "uploads", "outputs")
MAX_METADATA_BYTES = 2 * 1024 * 1024


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


@dataclass(frozen=True, repr=False)
class WorkspaceBoundaryIdentity:
    request_id: str
    user_id: str
    thread_id: str
    run_id: str
    agent_task_id: str
    generation: int
    attempt_id: str
    launch_spec_digest: str
    node_id: str
    node_session_id: str
    owner_worker_id: str
    token_stamp: str
    checkpoint_id: str
    kind: str
    publication_key: str
    presented_paths: tuple[str, ...]
    source_workspace_version: str
    checkpoint_ns: str = ""
    process_ref: str | None = None
    desired_core_status: str | None = None
    desired_task_status: str | None = None
    desired_placement_status: str | None = None
    error: str | None = None
    stop_reason: str | None = None

    @classmethod
    def from_context(cls, context, **boundary):
        from deerflow.runtime.execution.mutation_context import RemoteMutationContext

        if not isinstance(context, RemoteMutationContext):
            raise ValueError("Original remote mutation context required")
        return cls(**{f.name: getattr(context, f.name) for f in fields(context)}, **boundary)

    def __post_init__(self):
        for name in ("request_id", "user_id", "thread_id", "run_id", "agent_task_id", "attempt_id", "node_id", "node_session_id"):
            if not re.fullmatch(NAME_PATTERN, getattr(self, name)):
                raise ValueError("Invalid workspace identity")
        if type(self.generation) is not int or self.generation < 1 or self.checkpoint_ns != "" or self.kind not in {"partial", "final", "paused"}:
            raise ValueError("Invalid workspace boundary")
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", self.launch_spec_digest) or not re.fullmatch(r"[a-f0-9]{64}", self.token_stamp):
            raise ValueError("Invalid original execution digest")
        if self.process_ref is None:
            object.__setattr__(self, "process_ref", "fleet-" + self.attempt_id)
        if self.process_ref != "fleet-" + self.attempt_id:
            raise ValueError("Invalid original process reference")
        if self.owner_worker_id != "fleet-agent:" + self.attempt_id:
            raise ValueError("Invalid original execution owner")
        if any(not isinstance(value, str) or not value or len(value.encode()) > 128 for value in (self.checkpoint_id, self.publication_key, self.source_workspace_version)):
            raise ValueError("Incomplete workspace boundary")
        if not isinstance(self.presented_paths, tuple) or list(self.presented_paths) != sorted(set(self.presented_paths)):
            raise ValueError("Presented paths must be immutable and sorted")
        if len(self.presented_paths) > MAX_ENTRIES:
            raise ValueError("Presented path limit exceeded")
        for path in self.presented_paths:
            parts = relative_parts(path)
            if parts[0] != "outputs" or len(parts) < 2:
                raise ValueError("Presented paths must be output leaves")
        outcomes = (self.desired_core_status, self.desired_task_status, self.desired_placement_status)
        if self.kind == "partial" and (any(outcomes) or self.error or self.stop_reason):
            raise ValueError("Partial boundary cannot terminalize")
        if self.kind != "partial" and (
            self.desired_core_status not in {"success", "error", "interrupted", "timeout"}
            or self.desired_task_status not in {"succeeded", "failed", "cancelled", "timed_out", "paused", "input_required", "waiting_jobs"}
            or self.desired_placement_status not in {"succeeded", "failed", "cancelled", "timed_out"}
        ):
            raise ValueError("Final desired outcomes required")
        if self.desired_task_status == "waiting_jobs" and (self.kind != "final" or self.desired_core_status != "success" or self.desired_placement_status != "succeeded" or self.error is not None):
            raise ValueError("Waiting jobs requires successful final pair")

    def private_bytes(self):
        return canonical(asdict(self))

    def canonical_bytes(self):
        # Public/debug projection does not expose the private token stamp.
        value = asdict(self)
        value.pop("token_stamp")
        return canonical(value)

    @property
    def request_digest(self):
        return hashlib.sha256(self.private_bytes()).hexdigest()


class WorkspaceFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")
    path: str
    size: int = Field(ge=0, strict=True)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @field_validator("path")
    @classmethod
    def safe(cls, value):
        if relative_parts(value)[0] not in CATEGORIES:
            raise ValueError("Invalid workspace category")
        return value


class WorkspaceManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")
    schema_version: int = Field(default=1, ge=1, le=1)
    manifest_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    request_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    user_id: str
    thread_id: str
    agent_task_id: str
    run_id: str
    generation: int = Field(gt=0)
    attempt_id: str
    launch_spec_digest: str
    execution_digest: str = Field(pattern=r"^[a-f0-9]{64}$")
    categories: tuple[str, ...] = CATEGORIES
    directories: tuple[str, ...]
    files: tuple[WorkspaceFile, ...] = Field(max_length=MAX_ENTRIES)
    total_bytes: int = Field(ge=0, strict=True)
    nas_prefix: str

    @model_validator(mode="after")
    def consistent(self):
        if self.categories != CATEGORIES or list(self.directories) != sorted(set(self.directories)):
            raise ValueError("Noncanonical categories or directories")
        for value in (self.user_id, self.thread_id, self.agent_task_id, self.run_id, self.attempt_id):
            if not re.fullmatch(NAME_PATTERN, value):
                raise ValueError("Unsafe manifest owner")
        paths = [f.path for f in self.files]
        if paths != sorted(set(paths)) or self.total_bytes != sum(f.size for f in self.files):
            raise ValueError("Noncanonical workspace inventory")
        dirs = set(self.directories)
        if not set(CATEGORIES) <= dirs or dirs.intersection(paths) or len(dirs) + len(paths) > MAX_ENTRIES:
            raise ValueError("Invalid complete workspace inventory")
        for path in [*dirs, *paths]:
            parts = relative_parts(path)
            if parts[0] not in CATEGORIES or any("/".join(parts[:i]) not in dirs for i in range(1, len(parts))):
                raise ValueError("Incomplete category parents")
        if self.nas_prefix != "/".join((".fleet-agent-workspaces", self.user_id, self.thread_id, self.agent_task_id, self.attempt_id, self.manifest_id)):
            raise ValueError("Invalid C manifest prefix")
        if self.manifest_id != hashlib.sha256(canonical(self.content())).hexdigest():
            raise ValueError("Manifest content identity mismatch")
        return self

    def content(self):
        return self.model_dump(mode="json", exclude={"manifest_id", "nas_prefix"})


def bounded_names(fd, limit, *, omit=None):
    names = []
    with os.scandir(fd) as entries:
        for entry in entries:
            if entry.name == omit:
                continue
            names.append(entry.name)
            if len(names) > limit:
                raise ValueError("Workspace entry limit exceeded")
    return sorted(names)


def nonempty(fd):
    with os.scandir(fd) as entries:
        return next(entries, None) is not None


def signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size, info.st_ctime_ns, info.st_mtime_ns)


def no_replace(source, target, parent):
    """One kernel operation; never exists-then-rename or overwrite fallback."""
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        fn = libc.renameatx_np
        flag = 4
    elif sys.platform.startswith("linux") and hasattr(libc, "renameat2"):
        fn = libc.renameat2
        flag = 1
    else:
        raise RuntimeError("Atomic no-overwrite publication unsupported")
    fn.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    fn.restype = ctypes.c_int
    if fn(parent, os.fsencode(source), parent, os.fsencode(target), flag):
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code))


class AgentWorkspaceVersions:
    def __init__(self, nas, *, max_input_bytes, max_output_bytes):
        if any(type(x) is not int or x <= 0 for x in (max_input_bytes, max_output_bytes)):
            raise ValueError("Invalid workspace budgets")
        self.nas = nas
        self.max_input_bytes = max_input_bytes
        self.max_output_bytes = max_output_bytes

    def _inventory(self, root, *, destination=None, expected=None, snapshot=False):
        """Verify source descriptors and named entries before/after every copy."""
        rows = []
        dirs = []
        observations = []
        root_signature = signature(os.fstat(root))
        total = inputs = outputs = count = 0

        def walk(fd, prefix, target):
            nonlocal total, inputs, outputs, count
            before = signature(os.fstat(fd))
            names = bounded_names(fd, MAX_ENTRIES - count, omit="manifest.json" if not prefix and snapshot else None)
            for name in names:
                path = "/".join((*prefix, name))
                parts = relative_parts(path)
                count += 1
                if count > MAX_ENTRIES:
                    raise ValueError("Workspace entry limit exceeded")
                entry = os.stat(name, dir_fd=fd, follow_symlinks=False)
                observations.append((parts, signature(entry)))
                if stat.S_ISDIR(entry.st_mode):
                    child = os.open(name, OPEN_DIRECTORY, dir_fd=fd)
                    out = None
                    try:
                        if signature(os.fstat(child)) != signature(entry):
                            raise ValueError("Directory replacement during copy")
                        dirs.append(path)
                        if target is not None:
                            os.mkdir(name, mode=0o700, dir_fd=target)
                            out = os.open(name, OPEN_DIRECTORY, dir_fd=target)
                        walk(child, parts, out)
                        if signature(os.stat(name, dir_fd=fd, follow_symlinks=False)) != signature(entry):
                            raise ValueError("Directory changed during copy")
                    finally:
                        os.close(child)
                        if out is not None:
                            os.close(out)
                elif stat.S_ISREG(entry.st_mode):
                    leaf = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                    out = None
                    try:
                        original = os.fstat(leaf)
                        if signature(original) != signature(entry) or original.st_nlink != 1:
                            raise ValueError("Unsafe file replacement or hardlink")
                        limit = self.max_output_bytes if parts[0] == "outputs" else self.max_input_bytes
                        used = outputs if parts[0] == "outputs" else inputs
                        if used + original.st_size > limit:
                            raise ValueError("Workspace byte limit exceeded")
                        if target is not None:
                            out = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=target)
                        digest = hashlib.sha256()
                        size = 0
                        while chunk := os.read(leaf, min(65536, limit - used - size + 1)):
                            size += len(chunk)
                            if used + size > limit:
                                raise ValueError("Workspace byte limit exceeded")
                            digest.update(chunk)
                            if out is not None:
                                view = memoryview(chunk)
                                while view:
                                    view = view[os.write(out, view) :]
                        if size != original.st_size or signature(os.fstat(leaf)) != signature(original) or signature(os.stat(name, dir_fd=fd, follow_symlinks=False)) != signature(entry):
                            raise ValueError("Workspace source changed during copy")
                        if out is not None:
                            os.fsync(out)
                        rows.append(WorkspaceFile(path=path, size=size, sha256=digest.hexdigest()))
                        total += size
                        if parts[0] == "outputs":
                            outputs += size
                        else:
                            inputs += size
                    finally:
                        os.close(leaf)
                        if out is not None:
                            os.close(out)
                else:
                    raise ValueError("Only regular files and directories allowed")
            if bounded_names(fd, len(names), omit="manifest.json" if not prefix and snapshot else None) != names or signature(os.fstat(fd)) != before:
                raise ValueError("Directory changed during copy")
            if target is not None:
                os.fsync(target)

        # The fd is the original-grant user-data only, never the attempt root.
        if bounded_names(root, 4 if snapshot else 3) != sorted((*CATEGORIES, "manifest.json") if snapshot else CATEGORIES):
            raise ValueError("User-data category roots required")
        walk(root, (), destination)
        # Later-category reads must not hide replacement of an earlier entry.
        # Re-open each parent without following links, then compare the named
        # entry to its original descriptor identity after the complete copy.
        for parts, original in observations:
            parent = os.dup(root)
            try:
                for component in parts[:-1]:
                    child = os.open(component, OPEN_DIRECTORY, dir_fd=parent)
                    os.close(parent)
                    parent = child
                if signature(os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)) != original:
                    raise ValueError("Workspace entry changed during complete copy")
            finally:
                os.close(parent)
        if signature(os.fstat(root)) != root_signature:
            raise ValueError("Workspace root changed during complete copy")
        value = (tuple(sorted(dirs)), tuple(sorted(rows, key=lambda r: r.path)), total)
        if expected is not None and value != (expected.directories, expected.files, expected.total_bytes):
            raise ValueError("Workspace inventory or digest mismatch")
        return value

    @contextmanager
    def _parent(self, parts):
        # NASWorkspace owns sentinel/root no-follow validation; C additionally
        # persists each newly created ancestor before publishing below it.
        with self.nas.directory() as root:
            fd = os.dup(root)
            try:
                for part in parts:
                    relative_parts(part)
                    if "/" in part:
                        raise ValueError("Invalid C directory component")
                    try:
                        os.mkdir(part, mode=0o700, dir_fd=fd)
                    except FileExistsError:
                        pass
                    os.fsync(fd)
                    child = os.open(part, OPEN_DIRECTORY, dir_fd=fd)
                    os.close(fd)
                    fd = child
                yield fd
            finally:
                os.close(fd)

    def seal(self, identity, source_fd):
        if not isinstance(identity, WorkspaceBoundaryIdentity) or not stat.S_ISDIR(os.fstat(source_fd).st_mode):
            raise ValueError("Trusted source descriptor required")
        parts = (".fleet-agent-workspaces", identity.user_id, identity.thread_id, identity.agent_task_id, identity.attempt_id)
        with self._parent(parts) as parent:
            stage = ".candidate-" + uuid4().hex
            os.mkdir(stage, mode=0o700, dir_fd=parent)
            fd = os.open(stage, OPEN_DIRECTORY, dir_fd=parent)
            try:
                dirs, rows, total = self._inventory(source_fd, destination=fd)
                owner = {name: getattr(identity, name) for name in ("user_id", "thread_id", "agent_task_id", "run_id", "generation", "attempt_id", "launch_spec_digest")}
                private = {name: getattr(identity, name) for name in ("node_id", "node_session_id", "owner_worker_id", "token_stamp", "process_ref")}
                content = dict(
                    schema_version=1,
                    request_digest=identity.request_digest,
                    **owner,
                    execution_digest=hashlib.sha256(canonical(private)).hexdigest(),
                    categories=CATEGORIES,
                    directories=dirs,
                    files=[r.model_dump() for r in rows],
                    total_bytes=total,
                )
                mid = hashlib.sha256(canonical(content)).hexdigest()
                manifest = WorkspaceManifest(**content, manifest_id=mid, nas_prefix="/".join((*parts, mid)))
                data = canonical(manifest.model_dump(mode="json"))
                if len(data) > MAX_METADATA_BYTES:
                    raise ValueError("Workspace metadata budget exceeded")
                self._write(fd, "manifest.json", data)
                os.fsync(fd)
                try:
                    no_replace(stage, mid, parent)
                except OSError as exc:
                    if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                        raise
                os.fsync(parent)
                marker = ".request-" + hashlib.sha256(canonical([identity.attempt_id, identity.checkpoint_id, identity.kind, identity.publication_key])).hexdigest()
                pending = ".marker-" + uuid4().hex
                marker_data = canonical(dict(request_digest=identity.request_digest, manifest_id=mid))
                self._write(parent, pending, marker_data)
                try:
                    try:
                        no_replace(pending, marker, parent)
                    except OSError as exc:
                        if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                            raise
                        if self._read(parent, marker) != marker_data:
                            raise ValueError("Workspace request content conflict")
                finally:
                    try:
                        os.unlink(pending, dir_fd=parent)
                    except FileNotFoundError:
                        pass
                os.fsync(parent)
                return self.verify(manifest)
            finally:
                os.close(fd)
                try:
                    shutil.rmtree(stage, dir_fd=parent)
                except FileNotFoundError:
                    pass

    @staticmethod
    def _write(fd, name, data):
        leaf = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        try:
            view = memoryview(data)
            while view:
                view = view[os.write(leaf, view) :]
            os.fsync(leaf)
        finally:
            os.close(leaf)

    @staticmethod
    def _read(fd, name, *, missing_ok=False):
        try:
            leaf = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        except FileNotFoundError:
            if missing_ok:
                return None
            raise
        try:
            before = os.fstat(leaf)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > MAX_METADATA_BYTES:
                raise ValueError("Unsafe workspace metadata")
            data = b""
            while chunk := os.read(leaf, min(65536, MAX_METADATA_BYTES - len(data) + 1)):
                data += chunk
                if len(data) > MAX_METADATA_BYTES:
                    raise ValueError("Oversized workspace metadata")
            if signature(os.fstat(leaf)) != signature(before) or signature(os.stat(name, dir_fd=fd, follow_symlinks=False)) != signature(before):
                raise ValueError("Workspace metadata changed")
            return data
        finally:
            os.close(leaf)

    def _verify_fd(self, manifest, fd):
        stored = WorkspaceManifest.model_validate_json(self._read(fd, "manifest.json"))
        if stored != manifest:
            raise ValueError("Submitted workspace metadata mismatch")
        if bounded_names(fd, 4) != sorted((*CATEGORIES, "manifest.json")):
            raise ValueError("Unlisted workspace data")
        self._inventory(fd, expected=manifest, snapshot=True)
        if WorkspaceManifest.model_validate_json(self._read(fd, "manifest.json")) != manifest:
            raise ValueError("Workspace metadata changed during inventory verification")

    def verify(self, value):
        manifest = WorkspaceManifest.model_validate(value)
        with self.nas.directory(relative_parts(manifest.nas_prefix)) as fd:
            self._verify_fd(manifest, fd)
        return manifest

    def recover(self, identity):
        """Read only the original fixed request marker and fully verify its candidate."""
        if not isinstance(identity, WorkspaceBoundaryIdentity):
            raise ValueError("Original workspace recovery identity required")
        parts = (".fleet-agent-workspaces", identity.user_id, identity.thread_id, identity.agent_task_id, identity.attempt_id)
        marker = ".request-" + hashlib.sha256(canonical([identity.attempt_id, identity.checkpoint_id, identity.kind, identity.publication_key])).hexdigest()
        with self.nas.directory() as root:
            parent = os.dup(root)
            try:
                for component in parts:
                    try:
                        child = os.open(component, OPEN_DIRECTORY, dir_fd=parent)
                    except FileNotFoundError:
                        return None
                    os.close(parent)
                    parent = child
                marker_data = self._read(parent, marker, missing_ok=True)
                if marker_data is None:
                    return None
                value = json.loads(marker_data)
                if (
                    not isinstance(value, dict)
                    or set(value) != {"request_digest", "manifest_id"}
                    or value["request_digest"] != identity.request_digest
                    or not isinstance(value["manifest_id"], str)
                    or re.fullmatch(r"[a-f0-9]{64}", value["manifest_id"]) is None
                    or canonical(value) != marker_data
                ):
                    raise ValueError("Original workspace recovery marker conflicts")
                candidate_fd = os.open(value["manifest_id"], OPEN_DIRECTORY, dir_fd=parent)
                try:
                    manifest = WorkspaceManifest.model_validate_json(self._read(candidate_fd, "manifest.json"))
                finally:
                    os.close(candidate_fd)
                private = {name: getattr(identity, name) for name in ("node_id", "node_session_id", "owner_worker_id", "token_stamp", "process_ref")}
                if (
                    manifest.manifest_id != value["manifest_id"]
                    or manifest.nas_prefix != "/".join((*parts, value["manifest_id"]))
                    or manifest.request_digest != identity.request_digest
                    or manifest.execution_digest != hashlib.sha256(canonical(private)).hexdigest()
                    or any(getattr(manifest, name) != getattr(identity, name) for name in ("user_id", "thread_id", "agent_task_id", "run_id", "generation", "attempt_id", "launch_spec_digest"))
                ):
                    raise ValueError("Recovered candidate original identity conflicts")
                verified = self.verify(manifest)
                if self._read(parent, marker) != marker_data:
                    raise ValueError("Workspace recovery marker changed during verification")
                return verified
            finally:
                os.close(parent)

    def restore(self, value, destination_fd):
        manifest = WorkspaceManifest.model_validate(value)
        with self.nas.directory(relative_parts(manifest.nas_prefix)) as fd:
            self._verify_fd(manifest, fd)
            if nonempty(destination_fd):
                self._inventory(destination_fd, expected=manifest)
                return
            self._inventory(fd, destination=destination_fd, expected=manifest, snapshot=True)
            self._inventory(destination_fd, expected=manifest)

    def open_verified_output(self, value, relative_path):
        """Return owned verified bytes; response never reopens the NAS name."""
        import tempfile

        manifest = WorkspaceManifest.model_validate(value)
        parts = relative_parts(relative_path)
        if parts[0] != "outputs" or len(parts) < 2:
            raise ValueError("Only accepted output leaves can be downloaded")
        metadata = next((item for item in manifest.files if item.path == relative_path), None)
        if metadata is None:
            raise LookupError("Accepted output not found")
        spool = tempfile.TemporaryFile(mode="w+b")
        try:
            with self.nas.directory(relative_parts(manifest.nas_prefix)) as root:
                self._verify_fd(manifest, root)
                parent = os.dup(root)
                try:
                    for component in parts[:-1]:
                        child = os.open(component, OPEN_DIRECTORY, dir_fd=parent)
                        os.close(parent)
                        parent = child
                    leaf = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                    try:
                        before = os.fstat(leaf)
                        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size != metadata.size:
                            raise ValueError("Unsafe accepted output descriptor")
                        digest = hashlib.sha256()
                        size = 0
                        while chunk := os.read(leaf, min(65536, metadata.size - size + 1)):
                            size += len(chunk)
                            if size > metadata.size:
                                raise ValueError("Accepted output grew during read")
                            digest.update(chunk)
                            spool.write(chunk)
                        if size != metadata.size or digest.hexdigest() != metadata.sha256 or signature(os.fstat(leaf)) != signature(before) or signature(os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)) != signature(before):
                            raise ValueError("Accepted output changed during read")
                    finally:
                        os.close(leaf)
                finally:
                    os.close(parent)
                self._verify_fd(manifest, root)
            spool.seek(0)
            return spool, metadata
        except BaseException:
            spool.close()
            raise
