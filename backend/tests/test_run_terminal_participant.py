"""Trusted terminal participation on the original repository transaction."""

import pytest
from sqlalchemy import text
from test_run_repository import _cleanup, _make_repo


class Participant:
    def __init__(self, *, fail=False):
        self.calls = []
        self.fail = fail

    async def before_transition(self, session, *, run_id, status, error, stop_reason):
        self.calls.append(("before", session))
        assert session.in_transaction()

    async def after_transition(self, session, *, run_id, status, error, stop_reason):
        self.calls.append(("after", session))
        assert self.calls[0][1] is session
        assert await session.scalar(text("SELECT status FROM runs WHERE run_id=:id"), {"id": run_id}) == status
        if self.fail:
            raise ValueError("pair insert rejected")


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["update_status", "finalize_if_not_cancelled", "update_run_completion"])
@pytest.mark.parametrize("fail", [False, True])
async def test_original_terminal_transaction_participant(tmp_path, method, fail):
    repo = await _make_repo(tmp_path)
    try:
        await repo.create_thread_operation_atomic("r1", thread_id="t1", owner_worker_id="owner", lease_expires_at=None, user_id="u1")
        participant = Participant(fail=fail)
        repo._terminal_participant = participant
        if fail:
            with pytest.raises(ValueError, match="pair insert rejected"):
                await getattr(repo, method)("r1", status="success")
        else:
            await getattr(repo, method)("r1", status="success")
        assert [c[0] for c in participant.calls] == ["before", "after"]
        assert (await repo.get("r1", user_id="u1"))["status"] == ("pending" if fail else "success")
    finally:
        await _cleanup()


@pytest.mark.anyio
async def test_cancel_winner_does_not_accept_a_terminal_pair(tmp_path):
    repo = await _make_repo(tmp_path)
    try:
        await repo.create_thread_operation_atomic("r1", thread_id="t1", owner_worker_id="owner", lease_expires_at=None, user_id="u1")
        await repo.request_cancel("r1", action="interrupt")
        participant = Participant()
        repo._terminal_participant = participant
        result = await repo.finalize_if_not_cancelled("r1", status="success")
        assert result.finalized is False
        assert [c[0] for c in participant.calls] == ["before"]
        assert (await repo.get("r1", user_id="u1"))["status"] == "pending"
    finally:
        await _cleanup()
