"""Actual saver callback observes committed rows after original connection/lock."""

import pytest
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment


@pytest.mark.asyncio
async def test_original_saver_postcommit_callback_outside_lock(checkpoint_owner):
    item = checkpoint_owner
    called = []

    async def committed(config, metadata):
        assert not item.writer.lock.locked()
        assert item.writer._mutation.get() is None
        async with item.engine.connect() as conn:
            assert await conn.scalar(text("SELECT 1 FROM checkpoints WHERE thread_id=:t AND checkpoint_ns='' AND checkpoint_id=:c"), {"t": item.spec.thread_id, "c": config["configurable"]["checkpoint_id"]}) == 1
            assert await conn.scalar(text("SELECT count(*) FROM checkpoint_writes WHERE thread_id=:t"), {"t": item.spec.thread_id}) >= 1
        called.append(config)

    item.writer.after_root_commit = committed
    result = await item.writer.aput(item.config, item.checkpoint, {"source": "loop", "step": 1, "parents": {}}, {})
    assert called == [result], "original saver omits root postcommit callback"
    sub = {"configurable": dict(item.config["configurable"], checkpoint_ns="sub:task")}
    await item.writer.aput(sub, item.checkpoint, {"source": "loop", "step": 1, "parents": {}}, {})
    assert called == [result]
