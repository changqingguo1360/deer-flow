"""Actual original worker/repository SQL ordering; native, not installed proof."""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI, Request
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment


class NativeCleanupPending(RuntimeError):
    """Hard deadline failure retains actual owners; never authorizes close."""

    def __init__(self, native, owners):
        super().__init__("Original native CAS cleanup deadline exceeded with retained owners")
        self.native, self.owners = native, owners
        self.scope = native.scope


async def finish_native_cas(native, release, proof, *, hold_for_observation=False, observation_seconds=10, waiting=None, fixture_deadline=None):
    """Retain one cleanup owner through caller timeout/cancel; preserve primary."""
    permit_release = asyncio.Event()
    if not hold_for_observation:
        permit_release.set()
    owners = {role: task for role, task in native.teardown._phases.items() if role in {"original-executor", "control-observer"}}
    deadline = min(native.controller.execution_deadline, native.teardown.budget.deadline)
    if fixture_deadline is not None:
        # A test-only earlier bound is frozen before this cleanup owner starts.
        # It never changes the original budget or execution write authority.
        deadline = min(deadline, fixture_deadline)
    proof["deadline"] = deadline
    proof["original_owners"] = owners
    proof["phases"] = []

    async def settle():
        # The fixture wait and original joins share the earlier immutable bound.
        timeout_scope = asyncio.timeout_at(deadline)
        proof["deadline_scope"] = timeout_scope
        try:
            async with timeout_scope:
                await permit_release.wait()
                release()
                proof["phases"].append("barrier-released")
                runner_error = None
                result = None
                try:
                    result = await asyncio.shield(native.execute)
                except BaseException as error:
                    if not native.execute.done():
                        raise
                    runner_error = error
                    proof["runner_error"] = error
                assert all(task.done() for task in owners.values())
                assert not any(role in native.teardown._phases for role in ("original-executor", "control-observer"))
                proof["phases"].append("original-runner-executor-observer-settled")
                from .c08_installed_cleanup import settle_owned_cleanup

                def check_action_deadline():
                    if timeout_scope.expired() or asyncio.get_running_loop().time() >= deadline:
                        raise TimeoutError("Native cleanup action reached its frozen deadline")

                async def join_node():
                    check_action_deadline()
                    native.stop_node.set()
                    await asyncio.shield(native.node)
                    proof["phases"].append("node-joined")

                async def close_environment():
                    check_action_deadline()
                    with native.scope():
                        await native.environment.close()
                    proof["phases"].append("environment-closed")

                await settle_owned_cleanup([("node", join_node), ("environment", close_environment)], original_error=runner_error)
                assert native.execute.done() and native.node.done() and native.teardown.closed
                if runner_error is not None:
                    raise runner_error
                return result
        except BaseException as error:
            # Aggregation may contain timeout cancellation: classify at the
            # owner boundary and never let a later action close expired owners.
            if not timeout_scope.expired() and asyncio.get_running_loop().time() < deadline:
                raise
            # Preserve the actual owners and scope, not a fake done receipt or
            # early resource close. The caller gets a hard failed cleanup.
            raise NativeCleanupPending(native, {"runner": native.execute, "node": native.node, **owners, **native.teardown._phases}) from error

    cleanup = asyncio.create_task(settle(), name="c09-retained-native-CAS-cleanup")
    proof["cleanup"] = cleanup
    primary = None
    if waiting is not None:
        waiting.set()
    try:
        return await asyncio.wait_for(asyncio.shield(cleanup), observation_seconds)
    except TimeoutError as error:
        if cleanup.done():
            # A completed cleanup error came from its original owner, rather
            # than the short observation timer. Rethrow it exactly once.
            cleanup.result()
        primary = error
        proof["primary_error"] = error
        permit_release.set()
    except asyncio.CancelledError as error:
        primary = error
        proof["primary_error"] = error
        permit_release.set()
    # A second caller cancellation also cannot replace/cancel this same owner.
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as error:
            if primary is None:
                primary = error
                proof["primary_error"] = error
        except BaseException:
            break
    proof["cleanup_done"] = cleanup.done()
    proof["cleanup_cancelled"] = cleanup.cancelled()
    try:
        cleanup.result()
    except BaseException as cleanup_error:
        if primary is not None:
            raise BaseExceptionGroup("Original wait and retained native cleanup failures", [primary, cleanup_error])
        raise
    raise primary


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "order,action,premise_failure",
    [
        ("cancel-first", "rollback", False),
        ("cancel-first", "interrupt", False),
        ("completion-first", "rollback", False),
        ("cancel-first", "rollback", True),
        ("cancel-first", "rollback", "timeout"),
        ("cancel-first", "rollback", "external-cancel"),
        ("completion-first", "rollback", "runner-error"),
    ],
)
async def test_original_prepared_terminal_cas(checkpoint_owner, tmp_path, monkeypatch, order, action, premise_failure):
    from langgraph.store.memory import InMemoryStore

    from app.gateway.authz import AuthContext
    from app.gateway.routers.thread_runs import router
    from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
    from deerflow.runtime.execution.mutation_context import ExecutionWorkspaceFailure
    from deerflow.runtime.runs.manager import RunManager

    from .c09_integration_fixture import original_native_execution

    class FixturePremiseFailure(Exception):
        pass

    item = checkpoint_owner
    app = FastAPI()
    app.state.run_store = item.env[4].state.run_store
    app.state.fleet_ownership = item.env[4].state.fleet_ownership
    app.state.thread_store = MemoryThreadMetaStore(InMemoryStore())
    await app.state.thread_store.create(item.spec.thread_id, user_id=item.spec.user_id)
    app.state.run_manager = RunManager(store=app.state.run_store)
    app.include_router(router)

    @app.middleware("http")
    async def owner(request: Request, call_next):
        request.state.user = item.env[2]
        request.state.auth_source = "session"
        request.state.auth = AuthContext(user=item.env[2], permissions=["runs:cancel"])
        return await call_next(request)

    complete = asyncio.Event()
    native = await original_native_execution(item, tmp_path, monkeypatch, seed_rollback=True, complete_graph=complete)
    marker = Path("/tmp/c09-cas-" + item.accepted.attempt_id + ".json")
    release_file = marker.with_suffix(".release")
    try:
        from deerflow.persistence.run.sql import RunRepository

        monkeypatch.syspath_prepend(str(Path(__file__).parents[1]))
        from . import c09_stock_linux_fixture as fixture

        monkeypatch.setattr(fixture, "_CAS_CONTEXTS", {})
        monkeypatch.setattr(fixture, "_CAS_ORIGINAL", None)
        original = RunRepository.finalize_if_not_cancelled
        monkeypatch.setattr(RunRepository, "finalize_if_not_cancelled", original)
        with native.scope():
            fixture.install_cas_observer(order)
        if premise_failure == "runner-error":
            original_finalization = native.repository.finalize_if_not_cancelled

            async def fail_after_original(*args, **kwargs):
                result = await original_finalization(*args, **kwargs)
                assert result.finalized
                raise FixturePremiseFailure("Typed fixture fault after actual original committed terminal call")

            monkeypatch.setattr(native.repository, "finalize_if_not_cancelled", fail_after_original)
        native.release.set()  # original observer remains active throughout the race
        complete.set()
        async with asyncio.timeout(10):
            while not marker.exists():
                if native.execute.done():
                    native.execute.result()
                    pytest.fail("Original CAS barrier was not reached")
                await asyncio.sleep(0.01)
        observed = json.loads(marker.read_text())
        assert observed["identity"]["run_id"] == item.spec.run_id
        assert observed["identity"]["attempt_id"] == item.accepted.attempt_id
        assert observed["phase"] == ("before-original-cas" if order == "cancel-first" else "after-original-commit")
        prepared = native.publisher.terminal.prepared[0]
        async with item.env[1]() as session:
            row = (
                await session.execute(
                    text(
                        "SELECT r.status,r.cancel_action,t.state,p.state,t.accepted_workspace_point_id,p.final_workspace_point_id,a.stopped_at,res.state FROM runs r "
                        "JOIN fleet_run_placements p ON p.run_id=r.run_id JOIN fleet_agent_tasks t ON t.id=p.agent_task_id "
                        "JOIN fleet_attempts a ON a.id=p.active_attempt_id JOIN fleet_reservations res ON res.attempt_id=a.id"
                    )
                )
            ).one()
            points = await session.scalar(text("SELECT count(*) FROM fleet_workspace_points"))
        assert row[6] is None and row[7] != "released"
        if order == "cancel-first":
            assert row[0:2] == ("running", None) and points == 0
        else:
            assert row[0:4] == ("success", None, "finishing", "finishing")
            assert row[4] == row[5] and points == 1
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"csrf_token": "c09"}, headers={"X-CSRF-Token": "c09"}) as client:
            reply = await client.post(f"/api/threads/{item.spec.thread_id}/runs/{item.spec.run_id}/cancel?wait=false&action={action}")
            assert reply.status_code == (202 if order == "cancel-first" else 409), reply.text
            first = await app.state.run_store.get(item.spec.run_id)
            async with item.env[1]() as session:
                budget = (await session.execute(text("SELECT t.deadline,a.execution_deadline FROM fleet_agent_tasks t JOIN fleet_run_placements p ON p.agent_task_id=t.id JOIN fleet_attempts a ON a.id=p.active_attempt_id"))).one()
            first_deadline = None if first["cancel_requested_at"] is None else min(datetime.fromisoformat(first["cancel_requested_at"]) + timedelta(seconds=120), *budget)
            if premise_failure:
                raise FixturePremiseFailure("Deliberate post-intent assertion failure cleanup probe")
            duplicate = await client.post(f"/api/threads/{item.spec.thread_id}/runs/{item.spec.run_id}/cancel?wait=false&action={'interrupt' if action == 'rollback' else 'rollback'}")
            assert duplicate.status_code == reply.status_code, duplicate.text
            second = await app.state.run_store.get(item.spec.run_id)
            assert second["cancel_action"] == first["cancel_action"]
            assert second["cancel_requested_at"] == first["cancel_requested_at"]
            async with item.env[1]() as session:
                repeated_budget = (await session.execute(text("SELECT t.deadline,a.execution_deadline FROM fleet_agent_tasks t JOIN fleet_run_placements p ON p.agent_task_id=t.id JOIN fleet_attempts a ON a.id=p.active_attempt_id"))).one()
            assert repeated_budget == budget
            assert first_deadline == (None if second["cancel_requested_at"] is None else min(datetime.fromisoformat(second["cancel_requested_at"]) + timedelta(seconds=120), *repeated_budget))
        release_file.touch()
        result = await asyncio.wait_for(asyncio.shield(native.execute), 10)
        assert not result.ownership_lost
        assert result.status.value == ("success" if order == "completion-first" else "error" if action == "rollback" else "interrupted")
        assert result.error == ("Rolled back by user" if order == "cancel-first" and action == "rollback" else None)
        async with item.env[1]() as session:
            pair = (
                await session.execute(
                    text(
                        "SELECT w.kind,w.desired_core_status,w.checkpoint_id,w.request_id,t.accepted_workspace_point_id,p.final_workspace_point_id,r.cancel_action FROM fleet_workspace_points w "
                        "JOIN fleet_agent_tasks t ON t.id=w.agent_task_id JOIN fleet_run_placements p ON p.run_id=w.run_id JOIN runs r ON r.run_id=w.run_id"
                    )
                )
            ).one()
            assert await session.scalar(text("SELECT count(*) FROM fleet_workspace_points")) == 1
        assert pair[4] == pair[5]
        assert pair[0:2] == (("final", "success") if order == "completion-first" else ("paused", result.status.value))
        assert (pair[3] == prepared.request_id) == (order == "completion-first")
        assert pair[6] == (None if order == "completion-first" else action)
        async with item.env[1]() as session:
            stale = await session.scalar(text("SELECT state FROM fleet_workspace_requests WHERE id=:request"), {"request": prepared.request_id})
        assert (stale == "accepted") == (order == "completion-first")
        if order == "cancel-first" and action == "rollback":
            from deerflow.runtime.checkpoint_state import CheckpointStateAccessor

            with native.scope():
                restored = await CheckpointStateAccessor.bind(native.environment.agent_factory(), item.writer, mode="full").aget({"configurable": {"thread_id": item.spec.thread_id}})
            assert [m.id for m in restored.values["messages"]] == ["c09-before-run"]
            assert restored.values["title"] == "Pre-run title"
        assert native.source.joinpath("outputs/original.txt").read_bytes() == b"original workspace before interrupt\n"
    except FixturePremiseFailure:
        assert premise_failure
    finally:

        def release_owned():
            complete.set()
            release_file.touch()
            native.release.set()

        proof = {}
        probe = premise_failure if premise_failure in {"timeout", "external-cancel"} else None
        if probe is not None:
            waiting = asyncio.Event()
            outer = asyncio.create_task(
                finish_native_cas(native, release_owned, proof, hold_for_observation=True, observation_seconds=0.02 if probe == "timeout" else 10, waiting=waiting),
                name="c09-owned-cleanup-observation",
            )
            await waiting.wait()
            if probe == "external-cancel":
                outer.cancel("c09-specific-outer-observation")
            with pytest.raises(TimeoutError if probe == "timeout" else asyncio.CancelledError) as caught:
                await outer
            assert caught.value is proof["primary_error"]
            assert proof["cleanup_done"] and not proof["cleanup_cancelled"]
            assert set(proof["original_owners"]) == {"original-executor", "control-observer"}
            assert all(task.done() for task in proof["original_owners"].values())
            cleanup_result = native.execute.result()
            assert proof["phases"] == ["barrier-released", "original-runner-executor-observer-settled", "node-joined", "environment-closed"]
            print(
                json.dumps(
                    {
                        "event": "c09-native-cleanup-branch",
                        "branch": probe,
                        "primary_type": type(caught.value).__name__,
                        "exact_primary_preserved": True,
                        "same_cleanup_task_done": proof["cleanup"].done(),
                        "same_cleanup_task_cancelled": proof["cleanup"].cancelled(),
                        "phases": proof["phases"],
                        "original_owners_done": {role: task.done() for role, task in proof["original_owners"].items()},
                    }
                )
            )
        elif premise_failure == "runner-error":
            with pytest.raises(ExecutionWorkspaceFailure, match="Remote exact terminal pair failed") as caught:
                await finish_native_cas(native, release_owned, proof)
            assert caught.value is proof["runner_error"]
            assert proof["cleanup"].done() and not proof["cleanup"].cancelled()
            assert proof["phases"] == ["barrier-released", "original-runner-executor-observer-settled", "node-joined", "environment-closed"]
            assert all(task.done() for task in proof["original_owners"].values())
            print(json.dumps({"event": "c09-native-cleanup-branch", "branch": premise_failure, "primary_type": type(caught.value).__name__, "exact_primary_preserved": True, "phases": proof["phases"]}))
        else:
            cleanup_result = await finish_native_cas(native, release_owned, proof)
        marker.unlink(missing_ok=True)
        release_file.unlink(missing_ok=True)
        assert native.execute.done() and native.node.done()
        assert native.teardown.closed and not native.teardown._phases
    if premise_failure and premise_failure != "runner-error":
        assert cleanup_result.status.value == "error" and cleanup_result.error == "Rolled back by user"
        assert not cleanup_result.ownership_lost


@pytest.mark.asyncio
async def test_original_query_timeout_preserved_after_native_cleanup(checkpoint_owner, tmp_path, monkeypatch):
    """Real original bounded read times out acquiring an owned exhausted pool."""
    import time

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.fleet import agent_control

    from .c09_integration_fixture import original_native_execution

    item = checkpoint_owner
    complete = asyncio.Event()
    pool_engine = create_async_engine(item.engine.url, pool_size=1, max_overflow=0)
    native = await original_native_execution(item, tmp_path, monkeypatch, seed_rollback=True, complete_graph=complete)
    held = None
    captured = {}
    proof = {}
    try:
        held = await pool_engine.connect()
        sf = async_sessionmaker(pool_engine, expire_on_commit=False)

        original_read = agent_control.observe_original_control
        executor = (await native.manager.get(item.spec.run_id)).task
        assert executor is not None and not executor.done()

        async def original_bounded_read(session_factory, capability):
            if asyncio.current_task() is not executor or capability.context != native.publisher.capability.context:
                return await original_read(session_factory, capability)
            try:
                return await original_read(sf, capability)
            except TimeoutError as error:
                captured["original_error"] = error
                raise
            finally:
                await held.close()

        monkeypatch.setattr(agent_control, "observe_original_control", original_bounded_read)
        native.release.set()  # original observer still runs on its original pool
        with pytest.raises(TimeoutError) as caught:
            await finish_native_cas(native, complete.set, proof)
        assert caught.value is captured["original_error"] is proof["runner_error"]
        assert time.monotonic() < proof["deadline"] and not proof["deadline_scope"].expired()
        assert proof["cleanup"].done() and not proof["cleanup"].cancelled()
        assert all(task.done() for task in proof["original_owners"].values())
        assert proof["phases"] == ["barrier-released", "original-runner-executor-observer-settled", "node-joined", "environment-closed"]
        print(
            json.dumps(
                {
                    "event": "c09-native-cleanup-branch",
                    "branch": "original-query-timeout",
                    "primary_type": type(caught.value).__name__,
                    "exact_primary_preserved": True,
                    "deadline_unexpired": True,
                    "same_cleanup_task_done": True,
                    "same_cleanup_task_cancelled": False,
                    "phases": proof["phases"],
                }
            )
        )
    finally:
        if not native.execute.done():
            complete.set()
            native.release.set()
            await finish_native_cas(native, complete.set, {})
        assert native.execute.done() and native.node.done() and native.teardown.closed
        if held is not None:
            await held.close()
        await pool_engine.dispose()


@pytest.mark.asyncio
async def test_native_node_join_expiry_retains_actual_owner(checkpoint_owner, tmp_path, monkeypatch):
    """Earlier fixture bound only; not production120s/LaunchSpec expiry proof."""
    from .c09_integration_fixture import original_native_execution

    complete, poll_entered, poll_release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    native = await original_native_execution(checkpoint_owner, tmp_path, monkeypatch, seed_rollback=True, complete_graph=complete)
    close_calls = []
    proof = {}
    try:
        original_post = httpx.AsyncClient.post
        original_close = native.environment.close

        async def retained_original_poll(client, url, *args, **kwargs):
            result = await original_post(client, url, *args, **kwargs)
            if asyncio.current_task() is native.node and str(url).endswith("/poll"):
                poll_entered.set()
                await poll_release.wait()  # retains the SAME actual Node RPC Task
            return result

        async def observed_original_close():
            close_calls.append("original-environment-close")
            return await original_close()

        native.release.set()
        complete.set()
        await asyncio.wait_for(asyncio.shield(native.execute), 10)
        monkeypatch.setattr(httpx.AsyncClient, "post", retained_original_poll)
        monkeypatch.setattr(native.environment, "close", observed_original_close)
        await asyncio.wait_for(poll_entered.wait(), 3)
        fixture_deadline = asyncio.get_running_loop().time() + 0.05
        original_deadline = min(native.controller.execution_deadline, native.teardown.budget.deadline)
        assert fixture_deadline < original_deadline
        with pytest.raises(NativeCleanupPending) as caught:
            await finish_native_cas(native, complete.set, proof, fixture_deadline=fixture_deadline)
        pending = caught.value
        assert pending.native is native and pending.owners["node"] is native.node
        assert pending.scope == native.scope
        assert not native.node.done() and not native.node.cancelled()
        assert not close_calls and not native.teardown.closed
        assert proof["deadline_scope"].expired()
        assert proof["phases"] == ["barrier-released", "original-runner-executor-observer-settled"]
        assert proof["cleanup"].done() and not proof["cleanup"].cancelled()
        assert original_deadline == min(native.controller.execution_deadline, native.teardown.budget.deadline)
        print(
            json.dumps(
                {
                    "event": "c09-native-cleanup-branch",
                    "branch": "fixture-node-join-expiry",
                    "pending_type": type(pending).__name__,
                    "same_actual_node_retained": True,
                    "node_cancelled": False,
                    "fixture_close_calls_before_release": len(close_calls),
                    "phases": proof["phases"],
                    "scope": "Earlier frozen fixture cleanup bound only; original deadline and ordinary-write authority unchanged",
                }
            )
        )
    finally:
        # Release only this fixture wait, join the original Node, then perform
        # resource unwind; no re-admission or writes after the local expiry.
        poll_release.set()
        complete.set()
        native.release.set()
        if not native.execute.done():
            await finish_native_cas(native, complete.set, {})
        native.stop_node.set()
        await asyncio.shield(native.node)
        with native.scope():
            await native.environment.close()
        assert native.execute.done() and native.node.done() and native.teardown.closed
        assert not native.teardown._phases


@pytest.mark.anyio
async def test_original_present_files_delivery_prerequisite(tmp_path, monkeypatch):
    """Real graph/tool callbacks feed the original journal and output verdict."""
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
    from langgraph.graph import END, START, StateGraph
    from langgraph.prebuilt import ToolNode

    from deerflow.agents.thread_state import ThreadState
    from deerflow.config import paths
    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.journal import RunJournal
    from deerflow.runtime.runs.worker import _delivery_content_with_outputs, _delivery_error, _produced_output_paths
    from deerflow.tools.builtins.present_file_tool import present_file_tool
    from deerflow.workspace_changes import capture_workspace_snapshot

    thread = "c09-delivery-prerequisite"
    actual_paths = paths.Paths(tmp_path / "home")
    monkeypatch.setattr(paths, "_paths", actual_paths)
    actual_paths.ensure_thread_dirs(thread)
    outputs = actual_paths.sandbox_outputs_dir(thread)
    before = await capture_workspace_snapshot(thread, include_text=False)
    outputs.joinpath("parent.txt").write_bytes(b"original delivery prerequisite\n")
    produced = await _produced_output_paths(before, thread_id=thread, user_id=None)
    assert produced == ["/mnt/user-data/outputs/parent.txt"]
    store = MemoryRunEventStore()
    journal = RunJournal("c09-delivery", thread, store)
    # Only the model is deterministic. Tool execution and callback bookkeeping
    # are real: no direct journal callback or invented artifact receipt.
    model = FakeMessagesListChatModel(responses=[AIMessage(content="", tool_calls=[{"id": "c09-delivery-present", "name": "present_files", "args": {"filepaths": [str(outputs / "parent.txt")]}, "type": "tool_call"}])])

    async def request_presentation(state):
        return {"messages": [await model.ainvoke(state["messages"])]}

    graph = StateGraph(ThreadState)
    graph.add_node("request", request_presentation)
    graph.add_node("tools", ToolNode([present_file_tool]))
    graph.add_edge(START, "request")
    graph.add_edge("request", "tools")
    graph.add_edge("tools", END)
    try:
        assert _delivery_error(_delivery_content_with_outputs(journal.get_delivery_content(), produced)) == "Artifact delivery incomplete: no produced output artifact was presented"
        result = await graph.compile().ainvoke({"messages": [HumanMessage(content="Present the produced parent")], "thread_data": {"outputs_path": str(outputs)}}, config={"configurable": {"thread_id": thread}, "callbacks": [journal]})
        assert result["artifacts"] == produced
        assert any(isinstance(message, ToolMessage) and message.name == "present_files" and message.content == "Successfully presented files" for message in result["messages"])
        delivery = _delivery_content_with_outputs(journal.get_delivery_content(), produced)
        assert delivery["by_tool"] == {"present_files": produced}
        assert delivery["matched_paths"] == produced and delivery["satisfied"] is True
        assert _delivery_error(delivery) is None
        print(
            json.dumps(
                {"event": "c09-original-delivery-prerequisite", "real_tool": "present_files", "delivery": delivery, "scope": "native real graph/tool/journal/output snapshot; deterministic model only, not installed runner proof"},
                sort_keys=True,
            )
        )
    finally:
        await journal.flush()
