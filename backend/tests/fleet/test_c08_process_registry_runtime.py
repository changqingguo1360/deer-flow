"""Actual SQL wait bounds with original native Popen owners; no Linux PID claim."""

import asyncio
import subprocess
import sys
import time

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from .test_c02_remote_agent_admission import admission as admission  # noqa: F401
from .test_c03_remote_agent_admission import owner_environment as owner_environment  # noqa: F401
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner  # noqa: F401


@pytest.mark.asyncio
async def test_actual_process_register_lock_wait_cannot_outlive_original_execution_deadline(checkpoint_owner, monkeypatch):  # noqa: F811
    import app.fleet.workspace as module
    from app.fleet.mutation import FleetMutationCapability
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_process import OriginalToolProcess, supervisor_source_digest

    item = checkpoint_owner
    sync_engine = create_engine(item.engine.url.set(drivername="postgresql+psycopg"), connect_args={"options": "-csearch_path=" + item.private.database.postgres_schema})
    sf = sessionmaker(sync_engine)
    capability = FleetMutationCapability(item.identity, item.spec)
    registry = module.FleetWorkspaceProcessRegistry(sf, capability, pids_limit=item.grant["execution_profile"]["pids_limit"])
    registry.execution_deadline = time.monotonic() + 0.2
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    # macOS lacks procfs: this test isolates actual SQL waiting and the original
    # real handle. Actual PID/ticks/namespace are the separate Linux image gate.
    monkeypatch.setattr(module, "process_identity", lambda pid: {"pid": child.pid, "ppid": 1, "start_ticks": 100})
    record = OriginalToolProcess(pid=child.pid, start_ticks=100, role="supervisor", tool_execution_id="a" * 64, start_nonce="b" * 64, source_digest=supervisor_source_digest())

    def register():
        with remote_mutation_scope(capability.context):
            registry.register(record, child)

    blocker = await item.engine.connect()
    tx = await blocker.begin()
    await blocker.execute(text("SELECT id FROM fleet_agent_tasks FOR UPDATE"))
    writer = asyncio.create_task(asyncio.to_thread(register))
    try:
        await asyncio.sleep(0.4)
        assert writer.done(), "SQL registration ignored its original execution deadline while the owned Popen remained alive"
        assert child.poll() is None
        from sqlalchemy.exc import DBAPIError

        with pytest.raises(DBAPIError):
            await writer
        async with item.engine.connect() as connection:
            assert (await connection.execute(text("SELECT count(*) FROM fleet_workspace_processes"))).scalar_one() == 0
    finally:
        await tx.rollback()
        await blocker.close()
        await asyncio.gather(writer, return_exceptions=True)
        child.terminate()
        child.wait(timeout=3)
        sync_engine.dispose()
