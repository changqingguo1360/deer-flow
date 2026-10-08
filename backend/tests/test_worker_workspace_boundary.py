"""Stock single/multi worker remote hooks preserve Local defaults."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.worker import RunContext, run_agent


@pytest.mark.anyio
@pytest.mark.parametrize("modes", [["values"], ["values", "custom"]])
@pytest.mark.parametrize("remote", [False, True])
async def test_stock_astream_trusted_remote_sync_durability(modes, remote):
    manager = RunManager()
    record = await manager.create("workspace-thread")
    observed = []

    class Agent:
        async def astream(self, payload, **kwargs):
            observed.append(kwargs)
            if len(modes) == 1:
                yield {"messages": []}
            else:
                yield ("values", {"messages": []})

    context = RunContext(checkpointer=None, event_store=MemoryRunEventStore())
    object.__setattr__(context, "checkpoint_durability", "sync" if remote else None)
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    await run_agent(bridge, manager, record, ctx=context, agent_factory=lambda **_: Agent(), graph_input={}, config={}, stream_modes=modes)
    assert observed[0].get("durability") == ("sync" if remote else None)


@pytest.mark.anyio
async def test_remote_terminal_preparation_error_is_not_swallowed():
    manager = RunManager()
    record = await manager.create("workspace-thread")

    class Agent:
        async def astream(self, payload, **kwargs):
            yield {"messages": []}

    context = RunContext(checkpointer=None, event_store=MemoryRunEventStore())
    object.__setattr__(context, "prepare_terminal", AsyncMock(side_effect=ValueError("actual preparation fault")))
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    with pytest.raises(BaseException, match="preparation"):
        await run_agent(bridge, manager, record, ctx=context, agent_factory=lambda **_: Agent(), graph_input={}, config={}, stream_modes=["values"])
    context.prepare_terminal.assert_awaited_once()
    bridge.publish_end.assert_not_awaited()
