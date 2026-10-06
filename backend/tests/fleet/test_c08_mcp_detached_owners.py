"""Real SDK owners remain attached to their C scope across old pool detach paths."""

import asyncio
import threading
import time
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from pathlib import Path

import pytest

from .test_c02_remote_agent_admission import admission as admission  # noqa: F401
from .test_c03_remote_agent_admission import owner_environment as owner_environment  # noqa: F401
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner  # noqa: F401


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ("lru-cancel", "crossloop", "creation-unwind-cancel", "creation-double-cancel", "close-scope", "close-session", "close-if-current", "close-server", "close-all", "close-all-sync"))
async def test_actual_detached_sdk_owner_blocks_original_publication_sql_and_resources(checkpoint_owner, monkeypatch, path):  # noqa: F811
    import sys

    import langchain_mcp_adapters.sessions as sessions
    from deerflow_ecs_fleet.agent_workspace import WorkspaceBoundaryIdentity

    from app.fleet.mutation import FleetMutationCapability
    from app.fleet.runner_context import _AgentResourceTeardown, _pending_agent_teardowns
    from app.fleet.workspace import FleetWorkspacePublisher
    from deerflow.mcp.session_pool import MCPSessionPool
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope
    from deerflow.runtime.execution.workspace_boundary import WorkspaceWriterController, settled_workspace_activity, workspace_writer_scope

    item = checkpoint_owner
    capability = FleetMutationCapability(item.identity, item.spec)
    controller = WorkspaceWriterController()
    controller.execution_deadline = time.monotonic() + 10
    pool = MCPSessionPool()
    scope = capability.context.user_id + ":" + capability.context.thread_id

    @contextmanager
    def original_scope():
        with remote_mutation_scope(capability.context), workspace_writer_scope(controller):
            yield

    stack, unwound = AsyncExitStack(), []
    stack.callback(unwound.append, "original-private-resources")
    stack.push_async_callback(pool.close_all)
    teardown = _AgentResourceTeardown(stack, original_scope, capability.context)
    teardown.workspace_writers = controller
    publisher = FleetWorkspacePublisher(item.env[1], capability, controller=controller, teardown=teardown, session_pool=pool)
    identity = WorkspaceBoundaryIdentity.from_context(
        capability.context, request_id="actual-detached-" + path, checkpoint_id=item.config["configurable"]["checkpoint_id"], kind="partial", publication_key="presentation", presented_paths=(), source_workspace_version="original"
    )
    entered, release, initialized = threading.Event(), threading.Event(), threading.Event()
    original_create = sessions.create_session
    first = True

    @asynccontextmanager
    async def parked_sdk(connection):
        nonlocal first
        park = first
        first = False
        async with original_create(connection) as session:
            if park and path in {"creation-unwind-cancel", "creation-double-cancel"}:
                initialize = session.initialize

                async def failed_initialization():
                    await initialize()
                    if path == "creation-double-cancel":
                        initialized.set()
                        await asyncio.Event().wait()
                    raise ValueError("Actual initialized SDK creation rejected")

                monkeypatch.setattr(session, "initialize", failed_initialization)
            try:
                yield session
            finally:
                if park:
                    entered.set()
                    while not release.is_set():
                        await asyncio.sleep(0.005)

    monkeypatch.setattr(sessions, "create_session", parked_sdk)
    connection = {"transport": "stdio", "command": sys.executable, "args": [str(Path(__file__).with_name("c04_mcp_fixture.py"))], "env": {"ERP_AUTH": "c04-target-access"}}
    loop, thread = None, None
    original_owner = None
    publication = closing = creating = None

    @settled_workspace_activity
    async def original_session_call(server, request_scope):
        return await pool.get_session(server, request_scope, connection)

    async def wait_entered():
        async with asyncio.timeout(3):
            while not entered.is_set():
                await asyncio.sleep(0.005)

    try:
        with original_scope():
            if path == "crossloop":
                loop = asyncio.new_event_loop()
                running = threading.Event()

                def owner_loop():
                    asyncio.set_event_loop(loop)
                    running.set()
                    loop.run_forever()

                thread = threading.Thread(target=owner_loop, name="actual-c08-sdk-owner")
                thread.start()
                while not running.is_set():
                    await asyncio.sleep(0)
                session = await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(original_session_call("original", scope), loop))
                original_owner = pool._entries[("original", scope)][2]
                assert (await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(session.call_tool("echo", {"value": "original-foreign-loop"}), loop))).content[0].text == "original-foreign-loop"
                await original_session_call("original", scope)
                await wait_entered()
            elif path == "lru-cancel":
                session = await original_session_call("original", scope)
                original_owner = pool._entries[("original", scope)][2]
                assert (await session.call_tool("echo", {"value": "original-lru"})).content[0].text == "original-lru"
                pool.MAX_SESSIONS = 1
                # The requester has another scope. Its LRU victim must remain
                # bound to the victim's C scope, never the requester's scope.
                replacing = asyncio.create_task(original_session_call("another", "other-scope"))
                await wait_entered()
                replacing.cancel()
                with pytest.raises(BaseException):
                    await replacing
            elif path.startswith("close-"):
                session = await original_session_call("original", scope)
                original_owner = pool._entries[("original", scope)][2]
                assert (await session.call_tool("echo", {"value": "original-close"})).content[0].text == "original-close"
                if path == "close-all-sync":
                    pool.close_all_sync()
                    await wait_entered()
                else:
                    methods = {
                        "close-scope": lambda: pool.close_scope(scope),
                        "close-session": lambda: pool.close_session("original", scope),
                        "close-if-current": lambda: pool.close_session_if_current("original", scope, session),
                        "close-server": lambda: pool.close_server("original"),
                        "close-all": pool.close_all,
                    }
                    detaching = asyncio.create_task(methods[path]())
                    await wait_entered()
                    detaching.cancel()
                    with pytest.raises(BaseException):
                        await detaching
            else:
                creator_join_entered = asyncio.Event()
                original_shield = asyncio.shield

                def observe_creator_join(future):
                    inflight = pool._inflight.get(("original", scope))
                    if asyncio.current_task() is creating and inflight is not None and future is inflight[2]:
                        creator_join_entered.set()
                    return original_shield(future)

                monkeypatch.setattr(asyncio, "shield", observe_creator_join)
                creating = asyncio.create_task(original_session_call("original", scope))
                if path == "creation-double-cancel":
                    async with asyncio.timeout(3):
                        while not initialized.is_set():
                            await asyncio.sleep(0.005)
                    creating.cancel()

                await wait_entered()
                original_owner = pool._inflight[("original", scope)][2]
                # entered only proves the SDK cleanup is parked. The requester
                # must also be in its owner join before this cancellation;
                # otherwise cancellation at ready can enter that join forever.
                await asyncio.wait_for(creator_join_entered.wait(), timeout=3)
                creating.cancel()
                with pytest.raises((asyncio.CancelledError, ValueError)):
                    async with asyncio.timeout(3):
                        await creating
            assert not original_owner.done()
            assert not controller.unsettled, "The cancelled/returned tool activity really settled"
            sql_started = asyncio.Event()
            original_request = publisher.requests.create

            async def actual_request(*args, **kwargs):
                sql_started.set()
                return await original_request(*args, **kwargs)

            monkeypatch.setattr(publisher.requests, "create", actual_request)
            publication = asyncio.create_task(publisher.publish(identity))
            await asyncio.sleep(0.04)
            assert not sql_started.is_set(), "Actual request SQL ran while the detached SDK owner was alive"
            assert not publication.done()
            assert teardown.budget._deadline is None
            publication.cancel()
            await asyncio.gather(publication, return_exceptions=True)
            closing = asyncio.create_task(teardown.close())
            await asyncio.sleep(0.03)
            assert not teardown.closed and not unwound
            release.set()
            await asyncio.gather(closing, return_exceptions=True)
            assert original_owner.done() and not pool.scope_owners_pending(scope)
            assert teardown.closed and unwound == ["original-private-resources"]
    finally:
        release.set()
        if creating is not None:
            await asyncio.gather(creating, return_exceptions=True)
        if publication is not None:
            await asyncio.gather(publication, return_exceptions=True)
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)
        if original_owner is not None:

            async def join_owner():
                await asyncio.shield(original_owner)

            if loop is not None:
                await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(join_owner(), loop))
            else:
                await asyncio.gather(original_owner, return_exceptions=True)
        await stack.aclose()
        _pending_agent_teardowns.pop(capability.context, None)
        if loop is not None:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            assert not thread.is_alive()
            loop.close()
