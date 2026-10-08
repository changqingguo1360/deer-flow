"""Private, fsynced attempt records survive daemon restart; never mount on a job."""

import json
import os
import re
import stat
from pathlib import Path
from uuid import uuid4

from ..config import NAME_PATTERN

MAX_RECORD_BYTES = 1024 * 1024


class AttemptJournal:
    def __init__(self, state_dir: Path):
        self.root = Path(state_dir)

    def directory(self):
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
            raise PermissionError("Worker journal directory must be private and owned by the daemon")

    def save(self, record):
        attempt_id = record["claim"]["attempt_id"]
        if not re.fullmatch(NAME_PATTERN, attempt_id):
            raise ValueError("Invalid attempt journal identity")
        data = json.dumps(record, separators=(",", ":")).encode()
        if len(data) > MAX_RECORD_BYTES:
            raise ValueError("Attempt journal exceeds limit")
        self.directory()
        temporary = self.root / (".journal-" + uuid4().hex)
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, "wb") as file:
                file.write(data)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.root / (attempt_id + ".json"))
            fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)

    def records(self):
        self.directory()
        rows = []
        for path in self.root.glob("*.json"):
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, "rb") as file:
                info = os.fstat(file.fileno())
                if info.st_mode & 0o077 or not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_size > MAX_RECORD_BYTES:
                    raise PermissionError("Unsafe attempt journal file")
                row = json.load(file)
            if path.stem != row["claim"]["attempt_id"]:
                raise ValueError("Attempt journal identity mismatch")
            rows.append(row)
        return rows
