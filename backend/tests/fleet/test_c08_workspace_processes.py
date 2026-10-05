"""Actual PostgreSQL original-owner physical process identity invariants."""

from datetime import UTC, datetime

import pytest
from sqlalchemy import insert, select, update
from sqlalchemy.exc import DBAPIError

from .test_c08_workspace_transactions import boundary_db as boundary_db  # noqa: F401
from .test_c08_workspace_transactions import owner


def supervisor(**changes):
    return dict(**owner(), pid=101, start_ticks=10001, role="supervisor", tool_execution_id="actual-tool", start_nonce="c" * 64, source_digest="d" * 64) | changes


def shell(**changes):
    return supervisor(pid=102, start_ticks=10002, role="shell", supervisor_pid=101, supervisor_start_ticks=10001) | changes


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"supervisor_pid": None}, {"supervisor_start_ticks": None}, {"supervisor_pid": 999}, {"tool_execution_id": "another-tool"}, {"supervisor_role": "shell"}])
async def test_actual_shell_null_missing_cross_tool_or_wrong_role_parent_rolls_back(boundary_db, change):  # noqa: F811
    engine, _, models = boundary_db
    async with engine.begin() as conn:
        await conn.execute(insert(models.WorkspaceProcessRow).values(**supervisor()))
    with pytest.raises(DBAPIError):
        async with engine.begin() as conn:
            await conn.execute(insert(models.WorkspaceProcessRow).values(**shell(**change)))
    async with engine.connect() as conn:
        rows = (await conn.execute(select(models.WorkspaceProcessRow.__table__))).mappings().all()
        assert [(row["pid"], row["start_ticks"], row["role"]) for row in rows] == [(101, 10001, "supervisor")]


@pytest.mark.asyncio
async def test_actual_pid_reuse_distinct_start_and_identity_is_immutable(boundary_db):  # noqa: F811
    engine, _, models = boundary_db
    async with engine.begin() as conn:
        await conn.execute(insert(models.WorkspaceProcessRow).values(**supervisor()))
        await conn.execute(insert(models.WorkspaceProcessRow).values(**shell()))
        await conn.execute(insert(models.WorkspaceProcessRow).values(**supervisor(start_ticks=20001, tool_execution_id="next-tool", start_nonce="e" * 64)))
    for changes in ({"start_ticks": 20002}, {"tool_execution_id": "swap"}, {"token_stamp": "f" * 64}, {"source_digest": "f" * 64}):
        with pytest.raises(DBAPIError):
            async with engine.begin() as conn:
                await conn.execute(update(models.WorkspaceProcessRow).where(models.WorkspaceProcessRow.pid == 102).values(**changes))
    async with engine.begin() as conn:
        await conn.execute(update(models.WorkspaceProcessRow).where(models.WorkspaceProcessRow.pid == 102).values(state="settled", settled_at=datetime.now(UTC)))
    with pytest.raises(DBAPIError):
        async with engine.begin() as conn:
            await conn.execute(update(models.WorkspaceProcessRow).where(models.WorkspaceProcessRow.pid == 102).values(state="registered", settled_at=None))
    async with engine.connect() as conn:
        rows = (await conn.execute(select(models.WorkspaceProcessRow.__table__))).mappings().all()
        assert len(rows) == 3
        assert next(row for row in rows if row["pid"] == 102)["state"] == "settled"
