"""Worker restart must fail closed for unsafe journals before claiming work."""

import asyncio
import json
import time
from uuid import uuid4

import pytest
from deerflow_ecs_fleet.worker.daemon import NodeDaemon, RecoveryRequired
from deerflow_ecs_fleet.worker.journal import AttemptJournal
from deerflow_ecs_fleet.worker.watchdog import LeaseWatchdog


def test_journal_survives_restart_without_exposing_credentials(tmp_path):
    root = tmp_path / "state"
    record = {"claim": {"attempt_id": str(uuid4()), "token": "private-attempt-token"}, "node_id": "n", "reported": False}
    journal = AttemptJournal(root)
    journal.save(record)
    journal.save(record | {"reported": True})
    assert AttemptJournal(root).records() == [record | {"reported": True}]
    assert root.stat().st_mode & 0o077 == 0
    assert all(p.stat().st_mode & 0o077 == 0 for p in root.iterdir())
    assert not list(root.glob(".journal-*"))


@pytest.mark.parametrize("unsafe", ["public_directory", "public_record", "symlink_record", "symlink_directory", "mismatched_identity"])
def test_unsafe_journal_is_rejected(tmp_path, unsafe):
    root = tmp_path / "state"
    attempt_id = str(uuid4())
    row = {"claim": {"attempt_id": attempt_id}, "node_id": "n"}
    journal = AttemptJournal(root)
    journal.save(row)
    path = root / (attempt_id + ".json")
    if unsafe == "public_directory":
        root.chmod(0o755)
    elif unsafe == "public_record":
        path.chmod(0o644)
    elif unsafe == "symlink_record":
        other = tmp_path / "outside"
        other.write_text(json.dumps(row))
        path.unlink()
        path.symlink_to(other)
    elif unsafe == "symlink_directory":
        actual = tmp_path / "actual"
        root.rename(actual)
        root.symlink_to(actual, target_is_directory=True)
    else:
        path.write_text(json.dumps({"claim": {"attempt_id": str(uuid4())}}))
    with pytest.raises((PermissionError, ValueError, OSError)):
        journal.records()


@pytest.mark.asyncio
async def test_delayed_renewal_cannot_extend_local_execution():
    stopped = asyncio.Event()

    async def stop():
        stopped.set()
        return True

    watchdog = LeaseWatchdog(stop=stop, lease_seconds=1, safety_margin_seconds=0)
    sent = time.monotonic() - 2
    assert not watchdog.renew(1, request_started_at=sent)
    assert await asyncio.wait_for(watchdog.run(), timeout=0.2)
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_foreign_node_journal_blocks_engine_access_and_claim(tmp_path):
    AttemptJournal(tmp_path / "state").save({"claim": {"attempt_id": str(uuid4())}, "node_id": "other"})

    class Client:
        node_id = "n"

        async def open_session(self):
            return None

        async def claim(self):
            raise AssertionError("Cannot claim before reconciliation")

    class Containers:
        async def list_managed(self, node_id):
            raise AssertionError("Cannot touch another node's engine records")

    daemon = NodeDaemon(client=Client(), containers=Containers(), state_dir=tmp_path / "state", prepare_workspace=None)
    with pytest.raises(RecoveryRequired, match="another node"):
        await daemon.bootstrap()
    with pytest.raises(RecoveryRequired, match="reconcile"):
        await daemon.execute_one()
