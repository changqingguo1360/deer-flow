"""Original provider-selected cached saver must preserve root publication hooks."""

import pytest

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment
from .test_c08_stock_workspace_boundary import test_actual_stock_worker_graph_accepts_two_same_path_turns_then_final as _original_stock


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["full", "delta"])
async def test_provider_selected_saver_publishes_actual_stock_presentations(checkpoint_owner, tmp_path, monkeypatch, mode):
    import deerflow.runtime.checkpoint_mode as checkpoint_mode
    from app.fleet.runner_context import FleetCheckpointFence
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.checkpointer.cached_saver import CachedHistorySaver

    item = checkpoint_owner
    monkeypatch.setattr(checkpoint_mode, "_frozen_checkpoint_channel_mode", None)
    private = item.private.model_copy(update={"database": item.private.database.model_copy(update={"checkpoint_channel_mode": mode})})
    async with make_checkpointer(private, write_fence=FleetCheckpointFence(item.identity, item.spec)) as saver:
        assert isinstance(saver, CachedHistorySaver) is (mode == "delta")
        if mode == "delta":
            assert saver._cache.enabled
        item.writer = saver
        await _original_stock(item, tmp_path, monkeypatch, mode, ["values"], False)
        if mode == "delta":
            assert saver._inner.after_root_commit is saver.after_root_commit


@pytest.mark.asyncio
async def test_cached_callback_replacement_is_original_postcommit_hook(checkpoint_owner, monkeypatch):
    from sqlalchemy import text

    import deerflow.runtime.checkpoint_mode as checkpoint_mode
    from app.fleet.runner_context import FleetCheckpointFence
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer

    item = checkpoint_owner
    monkeypatch.setattr(checkpoint_mode, "_frozen_checkpoint_channel_mode", None)
    private = item.private.model_copy(update={"database": item.private.database.model_copy(update={"checkpoint_channel_mode": "delta"})})
    async with make_checkpointer(private, write_fence=FleetCheckpointFence(item.identity, item.spec)) as saver:
        called = []

        async def first(config, metadata):
            called.append("first")

        async def replacement(config, metadata):
            assert not saver._inner.lock.locked()
            assert saver._inner._mutation.get() is None
            async with item.engine.connect() as conn:
                assert await conn.scalar(text("SELECT count(*) FROM checkpoints WHERE thread_id=:t AND checkpoint_ns='' AND checkpoint_id=:c"), {"t": item.spec.thread_id, "c": config["configurable"]["checkpoint_id"]}) == 1
            called.append("replacement")

        saver.after_root_commit = first
        saver.after_root_commit = replacement
        result = await saver.aput(item.config, item.checkpoint, {"source": "loop", "step": 1, "parents": {}}, {})
        assert called == ["replacement"], "cache wrapper must bind the callback to the committing saver"
        assert saver._inner.after_root_commit is saver.after_root_commit is replacement
        sub = {"configurable": dict(item.config["configurable"], checkpoint_ns="sub:task")}
        await saver.aput(sub, item.checkpoint, {"source": "loop", "step": 1, "parents": {}}, {})
        assert called == ["replacement"]
        saver.after_root_commit = None
        await saver.aput(result, item.checkpoint, {"source": "loop", "step": 2, "parents": {}}, {})
        assert called == ["replacement"]
