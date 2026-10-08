"""SQLAlchemy-backed RunStore implementation.

Each method acquires and releases its own short-lived session.
Run status updates happen from background workers that may live
minutes -- we don't hold connections across long execution.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, case, func, inspect, or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.run.model import RunRow, ThreadExecutionBindingRow
from deerflow.runtime.execution.contracts import RunAdmissionParticipant, RunAdmissionUnitOfWork, RunTerminalParticipant
from deerflow.runtime.execution.mutation_context import reject_remote_operation, validate_mutation, validate_mutation_after_sql
from deerflow.runtime.runs.store.base import (
    LeaseRenewal,
    RunIdempotencyConflict,
    RunStore,
    StatusFinalization,
    normalize_run_created_at_iso,
)
from deerflow.runtime.user_context import AUTO, _AutoSentinel, resolve_user_id
from deerflow.utils.time import coerce_iso


def _lease_expired_or_null(lease_col, cutoff: datetime):
    """SQLAlchemy filter: True when the lease is NULL or has expired past *cutoff*."""
    return or_(lease_col.is_(None), lease_col < cutoff)


class RunRepository(RunStore):
    supports_admission_participants = True

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *, mutation_capability=None, terminal_participant: RunTerminalParticipant | None = None) -> None:
        self._sf = session_factory
        self._mutation_capability = mutation_capability
        self._terminal_participant = terminal_participant
        self._local_recovery_predicate = None
        self._thread_admission_guard = None
        self._before_thread_admission_guard = None

    def set_thread_admission_guard(self, guard):
        """Trusted application participant; invoked on the original admission TX."""
        self._thread_admission_guard = guard

    def set_before_thread_admission_guard(self, guard):
        """Host lock entry shared by run and checkpoint admission operations."""
        self._before_thread_admission_guard = guard

    async def _before_thread_admission(self, session, *, user_id, thread_id, participant=None):
        callback = getattr(participant, "before_thread_lock", None)
        if callback is not None:
            await callback(session)
        if self._before_thread_admission_guard is not None:
            await self._before_thread_admission_guard(session, user_id=user_id, thread_id=thread_id)

    async def _persisted_thread_backend(self, session, *, user_id, thread_id):
        """Follow existing owner-scoped server branch lineage parent first."""
        from deerflow.persistence.thread_meta.model import ThreadMetaRow
        from deerflow.runtime.runs.manager import ConflictError

        connection = await session.connection()
        has_metadata = await connection.run_sync(lambda conn: inspect(conn).has_table("threads_meta"))
        seen = set()
        current = thread_id
        lineage = []
        labels = set()
        branch_records = []
        for _ in range(64):
            if current in seen:
                raise ConflictError("Thread execution ancestry is inconsistent")
            seen.add(current)
            binding = await session.get(ThreadExecutionBindingRow, (user_id, current))
            if binding is not None:
                labels.add(binding.backend)
            history = (await session.execute(select(RunRow.kwargs_json).where(RunRow.user_id == user_id, RunRow.thread_id == current).order_by(RunRow.created_at))).scalars()
            labels.update(row.get("execution_backend") for row in history if row.get("execution_backend") not in (None, "local"))
            record = await session.get(ThreadMetaRow, current) if has_metadata else None
            metadata = record.metadata_json if record is not None else {}
            parent = metadata.get("branch_parent_thread_id") if metadata.get("deerflow_branch") is True else None
            if record is not None and record.user_id not in (None, user_id):
                raise ConflictError("Thread execution ancestry owner conflicts")
            lineage.append((current, parent))
            if parent is None:
                break
            if not isinstance(parent, str) or not parent or len(parent) > 64:
                raise ConflictError("Thread execution ancestry is malformed")
            branch_records.append((current, parent, record, metadata.get("branch_parent_checkpoint_id")))
            current = parent
        else:
            raise ConflictError("Thread execution ancestry exceeds its bound")
        if len(labels) > 1:
            raise ConflictError("Thread execution routing is inconsistent")
        if labels and branch_records:
            # Legacy metadata is only a hint. Validate each owner and original
            # checkpoint against server-written core execution history before
            # backfilling any independent routing rows.
            has_checkpoints = await connection.run_sync(lambda conn: inspect(conn).has_table("checkpoints"))
            if not has_checkpoints:
                raise ConflictError("Remote branch ancestry cannot be verified")
            for child, parent, record, checkpoint in branch_records:
                parent_record = await session.get(ThreadMetaRow, parent)
                if record is None or record.user_id != user_id or parent_record is None or parent_record.user_id != user_id or not isinstance(checkpoint, str) or not checkpoint:
                    raise ConflictError("Remote branch ancestry owner/checkpoint conflicts")
                root = await session.scalar(text("SELECT metadata FROM checkpoints WHERE thread_id=:thread AND checkpoint_ns='' AND checkpoint_id=:checkpoint"), {"thread": parent, "checkpoint": checkpoint})
                run = await session.get(RunRow, root.get("deerflow_execution_run_id")) if isinstance(root, dict) and root.get("deerflow_execution_run_id") else None
                if run is None or run.user_id != user_id or run.thread_id != parent or run.kwargs_json.get("execution_backend") not in labels:
                    raise ConflictError("Remote branch ancestry execution conflicts")
        return next(iter(labels), None), lineage

    async def _guard_thread_admission(self, session, *, user_id, thread_id, backend, operation, participant):
        if self._before_thread_admission_guard is not None:
            await self._before_thread_admission_guard(session, user_id=user_id, thread_id=thread_id)
        from deerflow.runtime.runs.manager import ConflictError

        if session.get_bind().dialect.name == "postgresql":
            await session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '|thread-execution|' || :key,0))"), {"key": json.dumps([user_id, thread_id])})
        binding = await session.get(ThreadExecutionBindingRow, (user_id, thread_id), with_for_update=True)
        if binding is None:
            historical, lineage = await self._persisted_thread_backend(session, user_id=user_id, thread_id=thread_id)
            label = historical or (backend if backend != "local" else None)
            if label is not None:
                # Persist ancestors before descendants; public metadata cannot
                # replace these independent host-owned routing rows.
                for ancestor, parent in reversed(lineage):
                    row = await session.get(ThreadExecutionBindingRow, (user_id, ancestor), with_for_update=True)
                    if row is None:
                        row = ThreadExecutionBindingRow(user_id=user_id, thread_id=ancestor, backend=label, parent_thread_id=parent)
                        session.add(row)
                        await session.flush()
                    elif row.backend != label:
                        raise ConflictError("Thread execution routing conflicts")
                    if ancestor == thread_id:
                        binding = row
        if binding is not None:
            if binding.recovery_required or (binding.parent_thread_id is not None and not binding.source_workspace):
                # A legacy routing backfill proves backend/ancestry only. It
                # cannot authorize a new branch run without its exact immutable
                # workspace origin; never treat it as an initial-input thread.
                raise ConflictError("Thread workspace requires recovery")
            guard = self._thread_admission_guard or getattr(participant, "guard_thread", None)
            if guard is None:
                raise ConflictError("Thread execution backend is unavailable")
            await guard(session, user_id=user_id, thread_id=thread_id, backend=binding.backend, requested_backend=backend, operation=operation, participant=participant)

    async def check_thread_admission(self, thread_id, *, user_id, operation="checkpoint_write", participant=None):
        resolved = resolve_user_id(user_id or AUTO, method_name="RunRepository.check_thread_admission")
        async with self._sf.begin() as session:
            await self._guard_thread_admission(session, user_id=resolved, thread_id=thread_id, backend="local", operation=operation, participant=participant)

    async def thread_execution_backend(self, thread_id, *, user_id):
        async with self._sf() as session:
            binding = await session.get(ThreadExecutionBindingRow, (user_id, thread_id))
            if binding is not None:
                return binding.backend
            backend, _ = await self._persisted_thread_backend(session, user_id=user_id, thread_id=thread_id)
            return backend

    def set_agent_run_control(self, control):
        """Install the trusted host-neutral original execution control adapter."""
        self._agent_run_control = control

    async def _terminal_hook(self, phase, session, *, run_id, status, error=None, stop_reason=None):
        if self._terminal_participant is not None and status in {"success", "error", "interrupted", "timeout"}:
            await session.connection()
            await getattr(self._terminal_participant, phase)(session, run_id=run_id, status=status, error=error, stop_reason=stop_reason)

    def set_local_recovery_predicate(self, predicate):
        """Trusted host SQL expression, always additional to the backend fence."""
        self._local_recovery_predicate = predicate

    def local_ownership_predicate(self):
        # This top-level backend label is server-owned admission output, never
        # selected from client metadata/config. Unknown labels fail closed.
        label = RunRow.kwargs_json["execution_backend"].as_string()
        predicate = or_(label.is_(None), label == "local")
        if self._local_recovery_predicate is not None:
            predicate = predicate & self._local_recovery_predicate
        return predicate

    @staticmethod
    def _normalize_model_name(model_name: str | None) -> str | None:
        """Normalize model_name for storage: strip whitespace, truncate to 128 chars."""
        if model_name is None:
            return None
        if not isinstance(model_name, str):
            model_name = str(model_name)
        normalized = model_name.strip()
        if len(normalized) > 128:
            normalized = normalized[:128]
        return normalized

    @staticmethod
    def _safe_json(obj: Any) -> Any:
        """Ensure obj is JSON-serializable. Falls back to model_dump() or str()."""
        if obj is None:
            return None
        if isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, dict):
            return {k: RunRepository._safe_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [RunRepository._safe_json(v) for v in obj]
        if hasattr(obj, "model_dump"):
            try:
                return obj.model_dump()
            except Exception:
                pass
        if hasattr(obj, "dict"):
            try:
                return obj.dict()
            except Exception:
                pass
        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)

    @staticmethod
    def _row_to_dict(row: RunRow) -> dict[str, Any]:
        d = row.to_dict()
        # Remap JSON columns to match RunStore interface
        d["metadata"] = d.pop("metadata_json", {})
        d["kwargs"] = d.pop("kwargs_json", {})
        # Convert datetime to ISO string for consistency with MemoryRunStore.
        # SQLite drops tzinfo on read despite ``DateTime(timezone=True)`` —
        # ``coerce_iso`` normalizes naive datetimes as UTC.
        for key in ("created_at", "updated_at", "lease_expires_at", "cancel_requested_at"):
            val = d.get(key)
            if isinstance(val, datetime):
                d[key] = coerce_iso(val)
        return d

    async def put(
        self,
        run_id,
        *,
        thread_id,
        assistant_id=None,
        user_id: str | None | _AutoSentinel = AUTO,
        model_name: str | None = None,
        status="pending",
        operation_kind: str = "run",
        multitask_strategy="reject",
        metadata=None,
        kwargs=None,
        error=None,
        stop_reason: str | None = None,
        created_at=None,
        follow_up_to_run_id=None,
        owner_worker_id: str | None = None,
        lease_expires_at: str | None = None,
        idempotency_key: str | None = None,
    ):
        """Insert or update a run row.

        ``RunManager`` retries ``put`` after transient SQLite failures.  Making
        this operation idempotent prevents a successful-but-unacknowledged first
        commit from turning the retry into a primary-key failure.
        """
        reject_remote_operation(self._mutation_capability)
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.put")
        now = datetime.now(UTC)
        created = datetime.fromisoformat(created_at) if created_at else now
        lease_dt = datetime.fromisoformat(lease_expires_at) if lease_expires_at else None
        values = {
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": resolved_user_id,
            "model_name": self._normalize_model_name(model_name),
            "status": status,
            "operation_kind": operation_kind,
            "multitask_strategy": multitask_strategy,
            "metadata_json": self._safe_json(metadata) or {},
            "kwargs_json": self._safe_json(kwargs) or {},
            "error": error,
            "stop_reason": stop_reason,
            "follow_up_to_run_id": follow_up_to_run_id,
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_dt,
            "idempotency_key": idempotency_key,
            "updated_at": now,
        }
        async with self._sf() as session:
            row = await session.get(RunRow, run_id)
            if row is None:
                session.add(RunRow(run_id=run_id, created_at=created, **values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)
            await session.commit()

    async def get(
        self,
        run_id,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.get")
        async with self._sf() as session:
            row = await session.get(RunRow, run_id)
            if row is None:
                return None
            if resolved_user_id is not None and row.user_id != resolved_user_id:
                return None
            return self._row_to_dict(row)

    async def list_by_thread(
        self,
        thread_id,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
        limit=100,
        before_created_at: str | None = None,
        before_run_id: str | None = None,
    ):
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.list_by_thread")
        stmt = select(RunRow).where(RunRow.thread_id == thread_id, RunRow.operation_kind == "run")
        if resolved_user_id is not None:
            stmt = stmt.where(RunRow.user_id == resolved_user_id)
        if before_created_at and before_run_id:
            cursor_dt = datetime.fromisoformat(normalize_run_created_at_iso(before_created_at))
            if cursor_dt.tzinfo is None:
                cursor_dt = cursor_dt.replace(tzinfo=UTC)
            else:
                cursor_dt = cursor_dt.astimezone(UTC)
            stmt = stmt.where(
                or_(
                    RunRow.created_at < cursor_dt,
                    and_(RunRow.created_at == cursor_dt, RunRow.run_id < before_run_id),
                )
            )
        # Keyset pages filter on (created_at, run_id) after thread_id. Existing
        # indexes are (thread_id) and (thread_id, status), so each page still
        # sorts matching rows. A covering (thread_id, created_at, run_id) index
        # is a follow-up if deep paging shows up in profiles.
        stmt = stmt.order_by(RunRow.created_at.desc(), RunRow.run_id.desc()).limit(limit)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(r) for r in result.scalars()]

    async def list_successful_regenerate_sources(
        self,
        thread_id,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.list_successful_regenerate_sources")
        source = RunRow.metadata_json["regenerate_from_run_id"].as_string()
        stmt = select(source).where(
            RunRow.thread_id == thread_id,
            RunRow.operation_kind == "run",
            RunRow.status == "success",
            source.is_not(None),
            source != "",
        )
        if resolved_user_id is not None:
            stmt = stmt.where(RunRow.user_id == resolved_user_id)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return {value for value in result.scalars() if isinstance(value, str) and value}

    async def list_edit_regenerate_runs(
        self,
        thread_id,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.list_edit_regenerate_runs")
        replay_kind = RunRow.metadata_json["replay_kind"].as_string()
        source = RunRow.metadata_json["regenerate_from_run_id"].as_string()
        stmt = select(RunRow).where(
            RunRow.thread_id == thread_id,
            replay_kind == "edit",
            source.is_not(None),
            source != "",
        )
        if resolved_user_id is not None:
            stmt = stmt.where(RunRow.user_id == resolved_user_id)
        stmt = stmt.order_by(RunRow.created_at.asc())
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(row) for row in result.scalars()]

    async def get_many_by_thread(
        self,
        thread_id,
        run_ids,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        if not run_ids:
            return {}
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.get_many_by_thread")
        stmt = select(RunRow).where(RunRow.thread_id == thread_id, RunRow.operation_kind == "run", RunRow.run_id.in_(run_ids))
        if resolved_user_id is not None:
            stmt = stmt.where(RunRow.user_id == resolved_user_id)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return {row.run_id: self._row_to_dict(row) for row in result.scalars()}

    async def update_status(self, run_id, status, *, error=None, stop_reason=None) -> bool:
        values: dict[str, Any] = {"status": status, "updated_at": datetime.now(UTC)}
        if error is not None:
            values["error"] = error
        if stop_reason is not None:
            values["stop_reason"] = stop_reason
        # Guard: only transition rows that are still active. ``interrupted`` is
        # included because the rollback path goes ``running → interrupted``
        # (cancel acknowledged) then ``interrupted → error`` (task finalize).
        # ``error`` and ``success`` remain locked so a peer's takeover (or a
        # completed run) cannot be overwritten by a late writer.
        async with self._sf() as session:
            await self._terminal_hook("before_transition", session, run_id=run_id, status=status, error=error, stop_reason=stop_reason)
            await validate_mutation(self._mutation_capability, session, "run.status", run_id=run_id, status=status, error=error, stop_reason=stop_reason)
            result = await session.execute(
                update(RunRow)
                .where(RunRow.run_id == run_id, RunRow.status.in_(("pending", "running", "interrupted") if self._mutation_capability is None else ("pending", "running", "interrupted", "success", "error", "timeout")))
                .values(**values)
            )
            if result.rowcount != 0:
                await self._terminal_hook("after_transition", session, run_id=run_id, status=status, error=error, stop_reason=stop_reason)
            await validate_mutation_after_sql(self._mutation_capability, session, "run.status", run_id=run_id, status=status, error=error, stop_reason=stop_reason)
            await session.commit()
            return result.rowcount != 0

    @staticmethod
    def owned_execution_predicates(run_id, *, user_id, thread_id, owner_worker_id, execution_backend):
        if not isinstance(execution_backend, str) or execution_backend in {"", "local"} or not owner_worker_id:
            raise ValueError("A trusted nonlocal executor identity is required")
        return (
            RunRow.run_id == run_id,
            RunRow.user_id == user_id,
            RunRow.thread_id == thread_id,
            RunRow.owner_worker_id == owner_worker_id,
            RunRow.kwargs_json["execution_backend"].as_string() == execution_backend,
            RunRow.status == "pending",
            RunRow.lease_expires_at > func.clock_timestamp(),
        )

    async def get_owned_execution(self, run_id, **identity):
        async with self._sf() as session:
            # Acquire the row before evaluating wallclock expiry. A predicate
            # evaluated before a lock wait can authorize an expired executor.
            await validate_mutation(self._mutation_capability, session, "run.attach", run_id=run_id, user_id=identity.get("user_id"), thread_id=identity.get("thread_id"))
            locked = (await session.execute(select(RunRow.run_id).where(RunRow.run_id == run_id).with_for_update())).scalar_one_or_none()
            if locked is None:
                return None
            row = (await session.execute(select(RunRow).where(*self.owned_execution_predicates(run_id, **identity)))).scalar_one_or_none()
            return self._row_to_dict(row) if row is not None else None

    async def start_owned_run(self, run_id, **identity):
        async with self._sf.begin() as session:
            await validate_mutation(self._mutation_capability, session, "run.start", run_id=run_id, user_id=identity.get("user_id"), thread_id=identity.get("thread_id"))
            locked = (await session.execute(select(RunRow.run_id).where(RunRow.run_id == run_id).with_for_update())).scalar_one_or_none()
            if locked is None:
                return False
            result = await session.execute(update(RunRow).where(*self.owned_execution_predicates(run_id, **identity)).values(status="running", updated_at=func.clock_timestamp()))
            await validate_mutation_after_sql(self._mutation_capability, session, "run.start", run_id=run_id, user_id=identity.get("user_id"), thread_id=identity.get("thread_id"))
            return result.rowcount != 0

    async def start_run(self, run_id: str) -> bool:
        """Start only a still-pending run; cancelled rows must not be resurrected."""
        reject_remote_operation(self._mutation_capability)
        async with self._sf() as session:
            result = await session.execute(
                update(RunRow)
                .where(
                    RunRow.run_id == run_id,
                    RunRow.status == "pending",
                )
                .values(status="running", updated_at=datetime.now(UTC))
            )
            await session.commit()
            return result.rowcount != 0

    async def update_model_name(self, run_id, model_name):
        async with self._sf() as session:
            await validate_mutation(self._mutation_capability, session, "run.model", run_id=run_id)
            await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(model_name=self._normalize_model_name(model_name), updated_at=datetime.now(UTC)))
            await validate_mutation_after_sql(self._mutation_capability, session, "run.model", run_id=run_id)
            await session.commit()

    async def delete(
        self,
        run_id,
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        reject_remote_operation(self._mutation_capability)
        resolved_user_id = resolve_user_id(user_id, method_name="RunRepository.delete")
        async with self._sf() as session:
            row = await session.get(RunRow, run_id)
            if row is None:
                return
            if resolved_user_id is not None and row.user_id != resolved_user_id:
                return
            await session.delete(row)
            await session.commit()

    async def delete_thread_operation(self, run_id: str, *, user_id: str | None) -> None:
        """Release a reservation using its captured owner, not request context."""
        reject_remote_operation(self._mutation_capability)
        await self.delete(run_id, user_id=user_id)

    async def list_pending(self, *, before=None):
        if before is None:
            before_dt = datetime.now(UTC)
        elif isinstance(before, datetime):
            before_dt = before
        else:
            before_dt = datetime.fromisoformat(before)
        stmt = select(RunRow).where(RunRow.operation_kind == "run", RunRow.status == "pending", RunRow.created_at <= before_dt).order_by(RunRow.created_at.asc())
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(r) for r in result.scalars()]

    async def list_inflight(self, *, before=None):
        """Return persisted active runs for startup recovery."""
        if before is None:
            before_dt = datetime.now(UTC)
        elif isinstance(before, datetime):
            before_dt = before
        else:
            before_dt = datetime.fromisoformat(before)
        stmt = (
            select(RunRow)
            .where(
                RunRow.status.in_(("pending", "running")),
                RunRow.created_at <= before_dt,
            )
            .order_by(RunRow.created_at.asc())
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(r) for r in result.scalars()]

    async def update_run_completion(
        self,
        run_id: str,
        *,
        status: str,
        total_input_tokens: int = 0,
        total_output_tokens: int = 0,
        total_tokens: int = 0,
        llm_call_count: int = 0,
        lead_agent_tokens: int = 0,
        subagent_tokens: int = 0,
        middleware_tokens: int = 0,
        token_usage_by_model: dict[str, dict[str, int]] | None = None,
        message_count: int = 0,
        last_ai_message: str | None = None,
        first_human_message: str | None = None,
        error: str | None = None,
    ) -> bool:
        """Update status + token usage + convenience fields on run completion.

        Returns ``False`` when the row is missing or already has a conflicting
        terminal outcome.
        """
        values: dict[str, Any] = {
            "status": status,
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_tokens,
            "llm_call_count": llm_call_count,
            "lead_agent_tokens": lead_agent_tokens,
            "subagent_tokens": subagent_tokens,
            "middleware_tokens": middleware_tokens,
            "token_usage_by_model": self._safe_json(token_usage_by_model) or {},
            "message_count": message_count,
            "updated_at": datetime.now(UTC),
        }
        if last_ai_message is not None:
            values["last_ai_message"] = last_ai_message[:2000]
        if first_human_message is not None:
            values["first_human_message"] = first_human_message[:2000]
        if error is not None:
            values["error"] = error
        allowed_sources = ["pending", "running"]
        if status not in allowed_sources:
            allowed_sources.append(status)
        if status == "error" and "interrupted" not in allowed_sources:
            allowed_sources.append("interrupted")
        async with self._sf() as session:
            await self._terminal_hook("before_transition", session, run_id=run_id, status=status, error=error)
            await validate_mutation(self._mutation_capability, session, "run.completion", run_id=run_id, status=status, error=error)
            result = await session.execute(
                update(RunRow)
                .where(
                    RunRow.run_id == run_id,
                    RunRow.status.in_(tuple(allowed_sources)),
                )
                .values(**values)
            )
            if result.rowcount != 0:
                await self._terminal_hook("after_transition", session, run_id=run_id, status=status, error=error)
            await validate_mutation_after_sql(self._mutation_capability, session, "run.completion", run_id=run_id, status=status, error=error)
            await session.commit()
            return result.rowcount != 0

    async def update_run_progress(
        self,
        run_id: str,
        *,
        total_input_tokens: int | None = None,
        total_output_tokens: int | None = None,
        total_tokens: int | None = None,
        llm_call_count: int | None = None,
        lead_agent_tokens: int | None = None,
        subagent_tokens: int | None = None,
        middleware_tokens: int | None = None,
        token_usage_by_model: dict[str, dict[str, int]] | None = None,
        message_count: int | None = None,
        last_ai_message: str | None = None,
        first_human_message: str | None = None,
    ) -> None:
        """Update token usage + convenience fields while a run is still active."""
        values: dict[str, Any] = {"updated_at": datetime.now(UTC)}
        optional_counters = {
            "total_input_tokens": total_input_tokens,
            "total_output_tokens": total_output_tokens,
            "total_tokens": total_tokens,
            "llm_call_count": llm_call_count,
            "lead_agent_tokens": lead_agent_tokens,
            "subagent_tokens": subagent_tokens,
            "middleware_tokens": middleware_tokens,
            "message_count": message_count,
        }
        for key, value in optional_counters.items():
            if value is not None:
                values[key] = value
        if token_usage_by_model is not None:
            values["token_usage_by_model"] = self._safe_json(token_usage_by_model) or {}
        if last_ai_message is not None:
            values["last_ai_message"] = last_ai_message[:2000]
        if first_human_message is not None:
            values["first_human_message"] = first_human_message[:2000]
        async with self._sf() as session:
            await validate_mutation(self._mutation_capability, session, "run.progress", run_id=run_id)
            await session.execute(update(RunRow).where(RunRow.run_id == run_id, RunRow.status == "running").values(**values))
            await validate_mutation_after_sql(self._mutation_capability, session, "run.progress", run_id=run_id)
            await session.commit()

    async def aggregate_tokens_by_thread(self, thread_id: str, *, include_active: bool = False) -> dict[str, Any]:
        """Aggregate token usage for a thread.

        ``by_model`` is reduced in Python from each row's ``token_usage_by_model``
        JSON column so subagent / middleware tokens land on the model that
        actually produced them (issue #3645). Rows written before that column
        existed fall back to ``RunRow.model_name`` + ``RunRow.total_tokens``,
        preserving the legacy lead-only behavior instead of dropping the data.

        Headline totals (``total_tokens``, ``total_input_tokens``,
        ``total_output_tokens``) and the ``by_caller`` bucket are summed from
        their own columns and are therefore unaffected by the JSON column being
        empty.
        """
        statuses = ("success", "error", "running") if include_active else ("success", "error")
        _completed = RunRow.status.in_(statuses)
        _thread = RunRow.thread_id == thread_id
        _run_operation = RunRow.operation_kind == "run"

        stmt = select(
            RunRow.model_name,
            RunRow.total_tokens,
            RunRow.total_input_tokens,
            RunRow.total_output_tokens,
            RunRow.lead_agent_tokens,
            RunRow.subagent_tokens,
            RunRow.middleware_tokens,
            RunRow.token_usage_by_model,
        ).where(_thread, _run_operation, _completed)

        async with self._sf() as session:
            rows = (await session.execute(stmt)).all()

        total_tokens = total_input = total_output = total_runs = 0
        lead_agent = subagent = middleware = 0
        by_model: dict[str, dict] = {}
        for r in rows:
            total_runs += 1
            total_tokens += r.total_tokens
            total_input += r.total_input_tokens
            total_output += r.total_output_tokens
            lead_agent += r.lead_agent_tokens
            subagent += r.subagent_tokens
            middleware += r.middleware_tokens

            # ``or {}`` covers rows written before ``token_usage_by_model``
            # existed (the column is NULL on a manual ALTER ADD COLUMN without
            # backfill); fresh rows always carry the journal-produced dict.
            usage_by_model = r.token_usage_by_model or {}
            if usage_by_model:
                for model, usage in usage_by_model.items():
                    entry = by_model.setdefault(model, {"tokens": 0, "runs": 0})
                    entry["tokens"] += usage.get("total_tokens", 0)
                    entry["runs"] += 1
            else:
                model = r.model_name or "unknown"
                entry = by_model.setdefault(model, {"tokens": 0, "runs": 0})
                entry["tokens"] += r.total_tokens
                entry["runs"] += 1

        return {
            "total_tokens": total_tokens,
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_runs": total_runs,
            "by_model": by_model,
            "by_caller": {
                "lead_agent": lead_agent,
                "subagent": subagent,
                "middleware": middleware,
            },
        }

    # ------------------------------------------------------------------
    # Multi-worker run ownership methods
    # ------------------------------------------------------------------

    async def update_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str,
    ) -> bool:
        reject_remote_operation(self._mutation_capability)
        lease_dt = datetime.fromisoformat(lease_expires_at)
        values: dict[str, Any] = {
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_dt,
            "updated_at": datetime.now(UTC),
        }
        async with self._sf() as session:
            result = await session.execute(update(RunRow).where(RunRow.run_id == run_id, self.local_ownership_predicate(), RunRow.owner_worker_id == owner_worker_id, RunRow.status.in_(("pending", "running"))).values(**values))
            await session.commit()
            return result.rowcount != 0

    async def renew_lease(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        lease_expires_at: str,
    ) -> LeaseRenewal:
        """Renew the owner lease and read cancellation intent atomically."""
        reject_remote_operation(self._mutation_capability)
        lease_dt = datetime.fromisoformat(lease_expires_at)
        async with self._sf() as session:
            result = await session.execute(
                update(RunRow)
                .where(
                    RunRow.run_id == run_id,
                    self.local_ownership_predicate(),
                    RunRow.owner_worker_id == owner_worker_id,
                    RunRow.status.in_(("pending", "running")),
                )
                .values(
                    lease_expires_at=lease_dt,
                    updated_at=datetime.now(UTC),
                )
                .returning(RunRow.run_id, RunRow.cancel_action)
            )
            row = result.first()
            await session.commit()
        if row is None:
            return LeaseRenewal(renewed=False)
        return LeaseRenewal(renewed=True, cancel_action=row.cancel_action)

    async def request_cancel(self, run_id: str, *, action: str) -> str | None:
        """Atomically persist the first cancellation action on an active run."""
        reject_remote_operation(self._mutation_capability)
        if action not in ("interrupt", "rollback"):
            raise ValueError(f"Unsupported cancellation action: {action}")
        now = datetime.now(UTC)
        async with self._sf() as session:
            result = await session.execute(
                update(RunRow)
                .where(
                    RunRow.run_id == run_id,
                    RunRow.status.in_(("pending", "running")),
                )
                .values(
                    cancel_action=case(
                        (RunRow.cancel_action.is_(None), action),
                        else_=RunRow.cancel_action,
                    ),
                    cancel_requested_at=case(
                        (RunRow.cancel_requested_at.is_(None), now),
                        else_=RunRow.cancel_requested_at,
                    ),
                    updated_at=now,
                )
                .returning(RunRow.cancel_action)
            )
            row = result.first()
            await session.commit()
        return row.cancel_action if row is not None else None

    async def finalize_if_not_cancelled(
        self,
        run_id: str,
        *,
        status: str,
        error: str | None = None,
        stop_reason: str | None = None,
    ) -> StatusFinalization:
        """Atomically let completion win only before cancellation."""
        values: dict[str, Any] = {
            "status": status,
            "updated_at": datetime.now(UTC),
        }
        if error is not None:
            values["error"] = error
        if stop_reason is not None:
            values["stop_reason"] = stop_reason

        async with self._sf() as session:
            observe = getattr(self._mutation_capability, "observe_cancellation_async", None)
            if observe is not None:
                action = await observe(session)
                if action is not None:
                    return StatusFinalization(finalized=False, cancel_action=action)
            await self._terminal_hook("before_transition", session, run_id=run_id, status=status, error=error, stop_reason=stop_reason)
            await validate_mutation(self._mutation_capability, session, "run.finalize", run_id=run_id, status=status, error=error, stop_reason=stop_reason)
            result = await session.execute(
                update(RunRow)
                .where(
                    RunRow.run_id == run_id,
                    RunRow.status.in_(("pending", "running")),
                    RunRow.cancel_action.is_(None),
                )
                .values(**values)
                .returning(RunRow.run_id)
            )
            if result.first() is not None:
                await self._terminal_hook("after_transition", session, run_id=run_id, status=status, error=error, stop_reason=stop_reason)
                await validate_mutation_after_sql(self._mutation_capability, session, "run.finalize", run_id=run_id, status=status, error=error, stop_reason=stop_reason)
                await session.commit()
                return StatusFinalization(finalized=True)

            current = await session.execute(select(RunRow.cancel_action).where(RunRow.run_id == run_id))
            cancel_action = current.scalar_one_or_none()
            await validate_mutation_after_sql(self._mutation_capability, session, "run.finalize", run_id=run_id, status=status, error=error, stop_reason=stop_reason)
            await session.commit()
            return StatusFinalization(
                finalized=False,
                cancel_action=cancel_action,
            )

    async def claim_for_takeover(
        self,
        run_id: str,
        *,
        grace_seconds: int,
        error: str,
        stop_reason: str | None = None,
    ) -> bool:
        reject_remote_operation(self._mutation_capability)
        cutoff = datetime.now(UTC) - timedelta(seconds=grace_seconds)
        values: dict[str, Any] = {
            "status": "error",
            "error": error,
            "updated_at": datetime.now(UTC),
        }
        if stop_reason is not None:
            values["stop_reason"] = stop_reason
        async with self._sf() as session:
            result = await session.execute(
                update(RunRow)
                .where(
                    RunRow.run_id == run_id,
                    self.local_ownership_predicate(),
                    RunRow.status.in_(("pending", "running")),
                    _lease_expired_or_null(RunRow.lease_expires_at, cutoff),
                )
                .values(**values)
            )
            await session.commit()
            return result.rowcount != 0

    async def list_inflight_with_expired_lease(
        self,
        *,
        before: str | None = None,
        grace_seconds: int = 10,
    ) -> list[dict[str, Any]]:
        if before is None:
            before_dt = datetime.now(UTC)
        elif isinstance(before, datetime):
            before_dt = before
        else:
            before_dt = datetime.fromisoformat(before)
        cutoff = datetime.now(UTC) - timedelta(seconds=grace_seconds)
        stmt = (
            select(RunRow)
            .where(
                RunRow.status.in_(("pending", "running")),
                self.local_ownership_predicate(),
                RunRow.created_at <= before_dt,
                _lease_expired_or_null(RunRow.lease_expires_at, cutoff),
            )
            .order_by(RunRow.created_at.asc())
        )
        async with self._sf() as session:
            result = await session.execute(stmt)
            return [self._row_to_dict(r) for r in result.scalars()]

    async def create_thread_operation_atomic(
        self,
        run_id: str,
        *,
        thread_id: str,
        owner_worker_id: str | None,
        lease_expires_at: str | None,
        operation_kind: str = "run",
        multitask_strategy: str = "reject",
        assistant_id: str | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        metadata: dict[str, Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        created_at: str | None = None,
        grace_seconds: int = 10,
        idempotency_key: str | None = None,
        participant: RunAdmissionParticipant | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Atomically create a run with cross-process thread-uniqueness.

        - For ``reject``: INSERT, let the partial unique index enforce
          single-active-run. Returns ``(row_dict, [])`` on success, raises
          ``IntegrityError`` on conflict.
        - For ``interrupt`` / ``rollback``: SELECT FOR UPDATE inflight
          rows for the thread, cancel them (unless their lease is still valid),
          then INSERT the new row — all in one transaction. Returns
          ``(row_dict, claimed_row_dicts)``.

        Returns:
            Tuple of ``(new_run_dict, claimed_run_dicts)``.
        """
        reject_remote_operation(self._mutation_capability)
        from deerflow.runtime.runs.manager import ConflictError

        resolved_user_id = resolve_user_id(user_id or AUTO, method_name="RunRepository.create_thread_operation_atomic")
        now = datetime.now(UTC)
        created = datetime.fromisoformat(created_at) if created_at else now
        lease_dt = datetime.fromisoformat(lease_expires_at) if lease_expires_at else None
        cutoff = now - timedelta(seconds=grace_seconds)

        values = {
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "user_id": resolved_user_id,
            "model_name": self._normalize_model_name(model_name),
            "status": "pending",
            "operation_kind": operation_kind,
            "multitask_strategy": multitask_strategy,
            "metadata_json": self._safe_json(metadata) or {},
            "kwargs_json": self._safe_json(kwargs) or {},
            "owner_worker_id": owner_worker_id,
            "lease_expires_at": lease_dt,
            "idempotency_key": idempotency_key,
            "created_at": created,
            "updated_at": now,
        }

        try:
            async with RunAdmissionUnitOfWork(self._sf).transaction() as session:
                await self._before_thread_admission(session, user_id=resolved_user_id, thread_id=thread_id, participant=participant)
                if session.get_bind().dialect.name == "postgresql":
                    await session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '|thread-execution|' || :key,0))"), {"key": json.dumps([resolved_user_id, thread_id])})
                if participant is not None and idempotency_key is not None:
                    existing = (await session.execute(select(RunRow).where(RunRow.idempotency_key == idempotency_key))).scalar_one_or_none()
                    if existing is not None:
                        stored = self._row_to_dict(existing)
                        await participant.validate_reuse(session, stored)
                        raise RunIdempotencyConflict(stored)
                await self._guard_thread_admission(
                    session,
                    user_id=resolved_user_id,
                    thread_id=thread_id,
                    backend=getattr(participant, "admission_backend", (kwargs or {}).get("execution_backend", "local")) if participant is not None else "local",
                    operation=operation_kind,
                    participant=participant,
                )
                if participant is not None:
                    await participant.prepare(session)
                now = await session.scalar(select(func.clock_timestamp())) if session.get_bind().dialect.name == "postgresql" else datetime.now(UTC)
                cutoff = now - timedelta(seconds=grace_seconds)
                values["updated_at"] = now
                claimed: list[dict[str, Any]] = []

                if multitask_strategy in ("interrupt", "rollback"):
                    stmt = (
                        select(RunRow)
                        .where(
                            RunRow.thread_id == thread_id,
                            RunRow.status.in_(("pending", "running")),
                        )
                        .with_for_update()
                    )
                    result = await session.execute(stmt)
                    for row in result.scalars():
                        local = (await session.execute(select(RunRow.run_id).where(RunRow.run_id == row.run_id, self.local_ownership_predicate()))).scalar_one_or_none()
                        if local is None:
                            raise ConflictError(f"Thread {thread_id} has an externally managed run")
                        lease_expired = False
                        if row.lease_expires_at is not None:
                            # SQLite drops tzinfo on read despite
                            # ``DateTime(timezone=True)`` (see ``_row_to_dict``).
                            # Treat naive values as UTC — same convention as
                            # ``coerce_iso`` — so the Python-side comparison
                            # against the aware ``cutoff`` does not raise
                            # ``TypeError: can't compare offset-naive and
                            # offset-aware datetimes`` when heartbeat is enabled
                            # on SQLite.
                            row_lease = row.lease_expires_at
                            if row_lease.tzinfo is None:
                                row_lease = row_lease.replace(tzinfo=UTC)
                            lease_expired = row_lease < cutoff
                            if row_lease >= cutoff and row.owner_worker_id != owner_worker_id:
                                # Live run owned by another worker — we cannot
                                # interrupt it and the partial unique index would
                                # reject our INSERT anyway. Surface as
                                # ConflictError so the caller gets a clean signal
                                # instead of a retry loop on IntegrityError.
                                raise ConflictError(f"Thread {thread_id} already has an active run owned by another worker")
                        if row.operation_kind != "run" and not lease_expired:
                            raise ConflictError(f"Thread {thread_id} has an active checkpoint write")
                        row.status = "interrupted"
                        row.error = "Cancelled by newer run"
                        row.owner_worker_id = owner_worker_id
                        row.updated_at = now
                        claimed.append(self._row_to_dict(row))
                new_row = RunRow(run_id=run_id, **values)
                session.add(new_row)
                await session.flush()
                admitted = self._row_to_dict(new_row)
                if participant is not None:
                    await participant.insert(session, admitted)
            return admitted, claimed
        except IntegrityError as exc:
            # The UoW has already rolled back core and participant writes.
            # A concurrent process may have committed the same idempotency key.
            if idempotency_key is not None:
                async with RunAdmissionUnitOfWork(self._sf).transaction() as session:
                    await self._before_thread_admission(session, user_id=resolved_user_id, thread_id=thread_id, participant=participant)
                    if session.get_bind().dialect.name == "postgresql":
                        await session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || '|thread-execution|' || :key,0))"), {"key": json.dumps([resolved_user_id, thread_id])})
                    existing = (await session.execute(select(RunRow).where(RunRow.idempotency_key == idempotency_key))).scalar_one_or_none()
                    if existing is not None:
                        stored = self._row_to_dict(existing)
                        if participant is not None:
                            await participant.validate_reuse(session, stored)
                        raise RunIdempotencyConflict(stored) from exc
            raise
