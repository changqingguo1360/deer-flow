"""Native original post-copy publication race; no installed Docker proof."""

import asyncio
import hashlib
import json
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, Request
from sqlalchemy import text

from .test_c05_remote_agent_runtime import admission as admission
from .test_c05_remote_agent_runtime import checkpoint_owner as original_checkpoint_owner
from .test_c05_remote_agent_runtime import owner_environment as owner_environment


@pytest_asyncio.fixture
async def checkpoint_owner(owner_environment):
    """Keep the original PG/checkpointer scopes until this test's owners settle."""
    async with asynccontextmanager(original_checkpoint_owner.__wrapped__)(owner_environment) as item:
        proof = item.publication_cleanup = {}
        try:
            yield item
        finally:
            cleanup = proof.get("cleanup")
            if cleanup is not None:
                while not cleanup.done():
                    try:
                        await asyncio.shield(cleanup)
                    except asyncio.CancelledError:
                        continue
                    except BaseException:
                        break
                native = proof["native"]
                step, publication, http = proof.get("step"), proof.get("publication"), proof.get("http")
                publication_done = publication is None or (not publication.pending_copies and not publication.pending_saves and (publication._join_task is None or publication._join_task.done()))
                if not (native.execute.done() and native.node.done() and native.teardown.closed and not native.teardown._phases and (step is None or step.done()) and publication_done and (http is None or http.is_closed)):
                    # A failed hard deadline retains the SAME actual resources.
                    # Never let automatic pytest fixture teardown close the DB.
                    proof["pending_scope"] = asyncio.get_running_loop().create_future()
                    print(json.dumps({"event": "c09-publication-cleanup-pending", "cleanup_done": cleanup.done(), "runner_done": native.execute.done(), "node_done": native.node.done(), "resources_closed": native.teardown.closed}))
                    while not proof["pending_scope"].done():
                        try:
                            await asyncio.shield(proof["pending_scope"])
                        except asyncio.CancelledError:
                            continue


async def retain_publication_cleanup(native, step, publication, http, release, proof, *, original_error=None):
    """One real cleanup Task; caller cancellation never cancels owned phases."""
    from .test_c09_terminal_cas import NativeCleanupPending, finish_native_cas

    proof.update(native=native, step=step, publication=publication, http=http)
    proof["phases"] = []
    proof["step_joining"] = asyncio.Event()
    proof["waiter_cancelled"] = asyncio.Event()
    deadline = min(native.controller.execution_deadline, native.teardown.budget.deadline)
    proof["deadline"] = deadline

    async def settle():
        errors = []
        release()
        proof["phases"].append("owned-barriers-released")
        proof["step_joining"].set()
        if step is not None:
            try:
                await asyncio.shield(step)
            except BaseException as error:
                if not step.done():
                    raise
                if error is not original_error:
                    errors.append(error)
        proof["phases"].append("original-step-settled")
        if publication is not None:
            try:
                await publication.join_writers()
            except BaseException as error:
                if publication.pending_copies or publication.pending_saves or (publication._join_task is not None and not publication._join_task.done()):
                    raise NativeCleanupPending(native, {"cleanup": proof["cleanup"], "step": step, "publication_join": publication._join_task}) from error
                if error is not original_error:
                    errors.append(error)
        proof["phases"].append("publication-writers-settled")
        try:
            await finish_native_cas(native, release, {})
        except BaseException as error:
            if not (native.execute.done() and native.node.done() and native.teardown.closed and not native.teardown._phases):
                raise
            if error is not original_error:
                errors.append(error)
        proof["phases"].append("original-native-owners-settled")
        if http is not None:
            await http.aclose()
        proof["phases"].append("http-closed")
        if errors:
            if len(errors) == 1:
                raise errors[0]
            raise BaseExceptionGroup("Original publication cleanup failures", errors)

    cleanup = asyncio.create_task(settle(), name="c09-retained-publication-cleanup")
    proof["cleanup"] = cleanup
    primary = None
    while not cleanup.done():
        try:
            completed, _ = await asyncio.wait({cleanup}, timeout=max(0, deadline - asyncio.get_running_loop().time()))
            if not completed:
                # No cancellation/replacement or scope unwind. Fixture owner
                # gate retains this Task and resources beyond the failed wait.
                pending = NativeCleanupPending(native, {"cleanup": cleanup, "step": step, **native.teardown._phases})
                proof["pending_error"] = pending
                raise pending
        except asyncio.CancelledError as error:
            if primary is None:
                primary = error
                proof["primary_error"] = error
            proof["waiter_cancelled"].set()
    try:
        cleanup.result()
    except BaseException as error:
        if primary is not None and error is not primary:
            raise BaseExceptionGroup("Original publication wait and cleanup failures", [primary, error])
        raise
    if primary is not None:
        raise primary


@pytest.mark.asyncio
@pytest.mark.parametrize("action,seam,cleanup_cancel", [(action, seam, False) for seam in ("claim", "prepared-before", "prepared-after") for action in ("rollback", "interrupt")] + [("rollback", "claim", True)])
async def test_original_prepared_copy_claim_discards_stale_success(checkpoint_owner, tmp_path, monkeypatch, action, seam, cleanup_cancel):
    import deerflow_ecs_fleet.worker.daemon as daemon_module
    import deerflow_ecs_fleet.worker.workspace_publication as publication_module
    from deerflow_ecs_fleet.worker.client import NodeClient
    from deerflow_ecs_fleet.worker.journal import AttemptJournal
    from deerflow_ecs_fleet.workspace import NASWorkspace
    from langgraph.store.memory import InMemoryStore

    import app.fleet.workspace as workspace_module
    from app.gateway.authz import AuthContext
    from app.gateway.routers.thread_runs import router
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
    from deerflow.runtime.runs.manager import RunManager

    from . import c09_stock_linux_fixture as fixture
    from .c09_integration_fixture import original_native_execution

    item = checkpoint_owner
    public = FastAPI()
    public.state.run_store = item.env[4].state.run_store
    public.state.fleet_ownership = item.env[4].state.fleet_ownership
    public.state.thread_store = MemoryThreadMetaStore(InMemoryStore())
    await public.state.thread_store.create(item.spec.thread_id, user_id=item.spec.user_id)
    public.state.run_manager = RunManager(store=public.state.run_store)
    public.include_router(router)

    @public.middleware("http")
    async def owner(request: Request, call_next):
        request.state.user = item.env[2]
        request.state.auth_source = "session"
        request.state.auth = AuthContext(user=item.env[2], permissions=["runs:cancel"])
        return await call_next(request)

    complete = asyncio.Event()
    native = await original_native_execution(item, tmp_path, monkeypatch, seed_rollback=True, complete_graph=complete)
    marker = Path("/tmp/c09-cas-" + item.accepted.attempt_id + ".json")
    release_file = marker.with_suffix(".release")
    copy_claim, release_claim = asyncio.Event(), asyncio.Event()
    release_verification = threading.Event()
    post_response, release_response = asyncio.Event(), asyncio.Event()
    step = None
    publication = None
    http = None
    proof = {"scope": "native original HTTP/PG/AgentRunner/AgentWorkspacePublication, native census adapter; not Docker/NodeDaemon execution", "action": action, "seam": seam, "service_errors": []}
    try:
        monkeypatch.syspath_prepend(str(Path(__file__).parents[1]))
        monkeypatch.setattr(fixture, "_CAS_CONTEXTS", {})
        monkeypatch.setattr(fixture, "_CAS_ORIGINAL", None)
        monkeypatch.setattr(RunRepository, "finalize_if_not_cancelled", RunRepository.finalize_if_not_cancelled)
        with native.scope():
            fixture.install_cas_observer("cancel-first")
        native.release.set()
        complete.set()
        async with asyncio.timeout(10):
            while not marker.exists():
                if native.execute.done():
                    native.execute.result()
                    pytest.fail("Original prepared success CAS barrier missing")
                await asyncio.sleep(0.01)
        boundary, manifest, epoch = native.publisher.terminal.prepared
        proof["request_id"] = boundary.request_id
        assert boundary.kind == "final" and boundary.desired_core_status == "success"
        async with item.env[1]() as session:
            before = (await session.execute(text("SELECT row_to_json(w) FROM fleet_workspace_requests w WHERE id=:id"), {"id": boundary.request_id})).scalar_one()
        assert before["state"] == "prepared"
        service = item.env[4].state.fleet_workspaces
        original_claim = service.claim

        async def observe_claim(**kwargs):
            try:
                return await original_claim(**kwargs)
            except BaseException as error:
                proof["service_errors"].append({"request_id": kwargs["request_id"], "error_type": type(error).__name__, "operation": "workspace.claim", "error": str(error)})
                raise

        monkeypatch.setattr(service, "claim", observe_claim)
        original_prepared = service.prepared

        async def observe_prepared(**kwargs):
            try:
                return await original_prepared(**kwargs)
            except BaseException as error:
                proof["service_errors"].append({"request_id": kwargs["request_id"], "error_type": type(error).__name__, "operation": "workspace.prepared", "error": str(error)})
                raise

        monkeypatch.setattr(service, "prepared", observe_prepared)
        if seam == "prepared-after":
            from deerflow_ecs_fleet.agent_workspace import AgentWorkspaceVersions

            loop = asyncio.get_running_loop()
            original_verify = AgentWorkspaceVersions.verify

            def hold_actual_verification(self, candidate):
                verified = original_verify(self, candidate)
                if candidate.request_digest == boundary.request_digest:
                    loop.call_soon_threadsafe(copy_claim.set)
                    if not release_verification.wait(10):
                        raise TimeoutError("Owned native verification seam was not released")
                return verified

            monkeypatch.setattr(AgentWorkspaceVersions, "verify", hold_actual_verification)
        http = httpx.AsyncClient(transport=httpx.ASGITransport(app=item.env[4]), base_url="http://native")
        client = NodeClient(gateway_url="http://native", credential=item.env[7].token, http_client=http)
        client.session_id = item.identity.node_session_id
        original_attempt = client.attempt
        claims = 0

        async def after_copy_claim(claim, operation, **fields):
            nonlocal claims
            if operation == "workspace/claim":
                claims += 1
                if claims == 2 and seam == "claim":
                    assert fields["request_id"] == boundary.request_id
                    copy_claim.set()
                    await release_claim.wait()
            if operation == "workspace/prepared" and seam == "prepared-before":
                assert fields["request_id"] == boundary.request_id
                copy_claim.set()
                await release_claim.wait()
            response = await original_attempt(claim, operation, **fields)
            if cleanup_cancel and operation == "workspace/claim" and claims == 2:
                assert response == {"stop": False, "request": None}
                post_response.set()
                await release_response.wait()
            return response

        monkeypatch.setattr(client, "attempt", after_copy_claim)

        async def native_census(grant, **kwargs):
            # Native host receipt only. Original physical NAS recovery/copy and
            # original step identity comparison still execute unchanged.
            assert native.source.is_dir() and kwargs["request"]["identity"]["request_id"] == boundary.request_id
            return {
                "container_id": "native-host",
                "started_at": "native",
                "image": "native-source",
                "launch_fingerprint": item.spec.payload_digest(),
                "receipt": {"runner": {"pid": os.getpid(), "ppid": os.getppid(), "start_ticks": 1, "uid": os.getuid(), "state": "R"}, "collector": {"pid_namespace": "native-host"}, "cgroup_digest": "native-host"},
            }

        cfg = item.env[3].config
        publication = publication_module.AgentWorkspacePublication(
            client=client, containers=SimpleNamespace(quiesce_workspace=native_census), nas=NASWorkspace(cfg.nas_root, identity=cfg.nas_identity), journal=AttemptJournal(tmp_path / "journal")
        )
        claim = {"kind": "agent", "attempt_id": item.accepted.attempt_id, "token": item.accepted.token}
        step = asyncio.create_task(publication.step(claim, item.grant, output_dir=native.source, record={"claim": claim}, deadline=native.controller.execution_deadline), name="c09-original-native-publication-step")
        async with asyncio.timeout(5):
            await copy_claim.wait()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=public), base_url="http://test", cookies={"csrf_token": "c09"}, headers={"X-CSRF-Token": "c09"}) as control:
            reply = await control.post(f"/api/threads/{item.spec.thread_id}/runs/{item.spec.run_id}/cancel?wait=false&action={action}")
            assert reply.status_code == 202, reply.text
        fields = {"request_id": boundary.request_id, "request_digest": boundary.request_digest, "barrier_epoch": epoch, "nonce": before["claim_nonce"]}
        if seam == "claim" and action == "rollback":
            proof["rejected_conflicts"] = []
            for delta in ({"nonce": "f" * 64}, {"request_digest": "f" * 64}, {"barrier_epoch": epoch + 1}, {"request_id": "0" * 64}, {"token": "wrong-original-token"}):
                with pytest.raises(httpx.HTTPStatusError) as rejected:
                    await original_attempt(claim, "workspace/claim", **(fields | delta))
                assert rejected.value.response.status_code in {403, 409}
                proof["rejected_conflicts"].append(list(delta)[0])
            with pytest.raises(httpx.HTTPStatusError):
                await original_attempt(claim, "workspace/prepared", **(fields | {"manifest": manifest.model_dump(mode="json"), "nonce": "f" * 64}))
            # The readonly helper also rejects a caller's already-frozen earlier
            # local bound. Real expired owner/lease HTTP fences are tested by the
            # unchanged original Node idle/protocol neighbors, not forged here.
            from datetime import UTC, datetime, timedelta

            from deerflow.runtime.execution.mutation_context import OwnershipRejected

            async with item.env[1]() as session:
                row = await service.requests._locked(session, boundary, barrier_epoch=epoch)
                with pytest.raises(OwnershipRejected):
                    await service._stale_prepared_success(session, row, boundary, nonce=fields["nonce"], deadline=datetime.now(UTC) - timedelta(seconds=1))
        release_claim.set()
        release_verification.set()
        if cleanup_cancel:
            async with asyncio.timeout(5):
                await post_response.wait()
            return  # Focused cleanup branch; not another business CAS proof.
        try:
            result = await asyncio.wait_for(asyncio.shield(step), 5)
        except httpx.HTTPStatusError as error:
            proof["step_error"] = type(error).__name__
            proof["http_status"] = error.response.status_code
            raise
        assert result is False
        async with item.env[1]() as session:
            after = (await session.execute(text("SELECT row_to_json(w) FROM fleet_workspace_requests w WHERE id=:id"), {"id": boundary.request_id})).scalar_one()
        assert after == before, "Readonly stale receipt must not change original prepared row or claim lease"
        release_file.touch()
        result = await asyncio.wait_for(asyncio.shield(native.execute), 10)
        assert not result.ownership_lost
        assert result.status.value == ("error" if action == "rollback" else "interrupted")
        async with item.env[1]() as session:
            points = (
                await session.execute(
                    text(
                        "SELECT w.kind,w.desired_core_status,w.request_id,t.accepted_workspace_point_id,p.final_workspace_point_id,r.status,r.error,r.cancel_action "
                        "FROM fleet_workspace_points w JOIN fleet_agent_tasks t ON t.id=w.agent_task_id "
                        "JOIN fleet_run_placements p ON p.run_id=w.run_id JOIN runs r ON r.run_id=w.run_id"
                    )
                )
            ).all()
        assert len(points) == 1 and points[0][0] == "paused" and points[0][2] != boundary.request_id
        assert points[0][2] == points[0][3] == points[0][4]
        assert points[0][5:] == (result.status.value, "Rolled back by user" if action == "rollback" else None, action)
        proof["readonly_row_unchanged"] = True
        proof["accepted_winning_paused_pair"] = True
    finally:
        import sys

        primary = sys.exception()

        def release():
            release_claim.set()
            release_verification.set()
            complete.set()
            release_file.touch()
            native.release.set()

        cleanup_proof = item.publication_cleanup
        if cleanup_cancel and post_response.is_set():
            outer = asyncio.create_task(retain_publication_cleanup(native, step, publication, http, release, cleanup_proof, original_error=primary), name="c09-owned-publication-cleanup-waiter")
            try:
                await asyncio.sleep(0)
                while "step_joining" not in cleanup_proof:
                    await asyncio.sleep(0)
                await cleanup_proof["step_joining"].wait()
                outer.cancel("c09-only-publication-cleanup-waiter")
                await cleanup_proof["waiter_cancelled"].wait()
                assert not step.done() and not step.cancelled()
                assert not cleanup_proof["cleanup"].done() and not cleanup_proof["cleanup"].cancelled()
                assert not http.is_closed
                async with item.env[1]() as session:
                    assert await session.scalar(text("SELECT 1")) == 1
            finally:
                release_response.set()
                with pytest.raises(asyncio.CancelledError) as caught:
                    await outer
                assert caught.value is cleanup_proof["primary_error"]
            assert cleanup_proof["phases"] == ["owned-barriers-released", "original-step-settled", "publication-writers-settled", "original-native-owners-settled", "http-closed"]
            proof["cleanup_control"] = {
                "same_cleanup_task_done": cleanup_proof["cleanup"].done(),
                "cleanup_task_cancelled": cleanup_proof["cleanup"].cancelled(),
                "step_cancelled": step.cancelled(),
                "exact_primary_preserved": True,
                "http_pg_alive_while_pending": True,
                "phases": cleanup_proof["phases"],
            }
        else:
            release_response.set()
            await retain_publication_cleanup(native, step, publication, http, release, cleanup_proof, original_error=primary)
        marker.unlink(missing_ok=True)
        release_file.unlink(missing_ok=True)
        proof["host_modules"] = {m.__name__: {"path": m.__file__, "sha256": hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()} for m in (workspace_module, publication_module, daemon_module)}
        proof["owners_settled"] = native.execute.done() and native.node.done() and native.teardown.closed
        print(json.dumps(proof))
