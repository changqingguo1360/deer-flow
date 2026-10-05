"""Ownership-fenced adapter for the audited PostgreSQL saver write surface."""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from importlib.metadata import version

from langgraph.checkpoint.postgres import _ainternal
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row

from deerflow.runtime.execution.fence import ExecutionWriteFence

_REQUIRED = {
    "checkpoint_migrations": {"v"},
    "checkpoints": {"thread_id", "checkpoint_ns", "checkpoint_id", "parent_checkpoint_id", "type", "checkpoint", "metadata"},
    "checkpoint_blobs": {"thread_id", "checkpoint_ns", "channel", "version", "type", "blob"},
    "checkpoint_writes": {"thread_id", "checkpoint_ns", "checkpoint_id", "task_id", "idx", "channel", "type", "blob", "task_path"},
}


class FencedAsyncPostgresSaver(AsyncPostgresSaver):
    def __init__(self, conn, *, write_fence: ExecutionWriteFence, schema: str = ""):
        if version("langgraph-checkpoint-postgres") != "3.1.1":
            raise RuntimeError("Unsupported fenced PostgreSQL saver version")
        super().__init__(conn)
        self._write_fence = write_fence
        self._schema = schema
        self.after_root_commit = None
        self._mutation = ContextVar("checkpoint_mutation", default=None)

    async def setup(self):
        # Operators initialize migrations through the unfenced trusted factory.
        # Remote executors may only inspect the installed schema, never issue DDL.
        async with self.lock, _ainternal.get_connection(self.conn) as conn:
            async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SET TRANSACTION READ ONLY")
                await cur.execute("SELECT current_schema() AS schema")
                schema = (await cur.fetchone())["schema"]
                if not schema or (self._schema and schema != self._schema):
                    raise RuntimeError("Checkpoint schema is not ready")
                await cur.execute("SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema=%s AND table_name=ANY(%s)", (schema, list(_REQUIRED)))
                found = {}
                for row in await cur.fetchall():
                    found.setdefault(row["table_name"], set()).add(row["column_name"])
                    column = row["column_name"]
                    expected_type = "integer" if column in {"idx", "v"} else "jsonb" if column in {"checkpoint", "metadata"} else "bytea" if column == "blob" else "text"
                    nullable = (row["table_name"], column) in {("checkpoints", "parent_checkpoint_id"), ("checkpoints", "type"), ("checkpoint_blobs", "blob"), ("checkpoint_writes", "type")}
                    if column in _REQUIRED[row["table_name"]] and (row["data_type"] != expected_type or (row["is_nullable"] == "YES") != nullable):
                        raise RuntimeError("Checkpoint column schema is not ready")
                if any(not columns <= found.get(table, set()) for table, columns in _REQUIRED.items()):
                    raise RuntimeError("Checkpoint schema is not ready")
                primary_keys = {
                    "checkpoint_migrations": ["v"],
                    "checkpoints": ["thread_id", "checkpoint_ns", "checkpoint_id"],
                    "checkpoint_blobs": ["thread_id", "checkpoint_ns", "channel", "version"],
                    "checkpoint_writes": ["thread_id", "checkpoint_ns", "checkpoint_id", "task_id", "idx"],
                }
                await cur.execute(
                    "SELECT c.relname AS table_name, array_agg(a.attname ORDER BY k.ordinality) AS columns FROM pg_constraint p JOIN pg_class c ON "
                    "c.oid=p.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY AS "
                    "k(attnum,ordinality) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum WHERE n.nspname=%s AND p.contype='p' AND "
                    "c.relname=ANY(%s) GROUP BY c.relname",
                    (schema, list(_REQUIRED)),
                )
                if {row["table_name"]: row["columns"] for row in await cur.fetchall()} != primary_keys:
                    raise RuntimeError("Checkpoint primary key schema is not ready")
                await cur.execute(
                    "SELECT c.relname AS table_name, i.indisvalid, i.indisready, array_agg(a.attname ORDER BY k.ordinality) AS columns FROM pg_index i "
                    "JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY "
                    "AS k(attnum,ordinality) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum WHERE n.nspname=%s AND c.relname=ANY(%s) "
                    "GROUP BY c.relname,i.indexrelid,i.indisvalid,i.indisready",
                    (schema, ["checkpoints", "checkpoint_blobs", "checkpoint_writes"]),
                )
                indexed = {row["table_name"] for row in await cur.fetchall() if row["indisvalid"] and row["indisready"] and row["columns"] == ["thread_id"]}
                if indexed != {"checkpoints", "checkpoint_blobs", "checkpoint_writes"}:
                    raise RuntimeError("Checkpoint index schema is not ready")
                from psycopg import sql

                await cur.execute(sql.SQL("SELECT v FROM {}.checkpoint_migrations").format(sql.Identifier(schema)))
                if {row["v"] for row in await cur.fetchall()} != set(range(len(self.MIGRATIONS))):
                    raise RuntimeError("Checkpoint migrations are not ready")

    @asynccontextmanager
    async def _operation(self, thread_id, operation):
        token = self._mutation.set((str(thread_id), operation))
        try:
            yield
        finally:
            self._mutation.reset(token)

    async def aput(self, config, checkpoint, metadata, new_versions):
        from deerflow.runtime.execution.mutation_context import current_remote_mutation_context

        context = current_remote_mutation_context()
        if context is not None:
            metadata = dict(metadata, deerflow_execution_run_id=context.run_id)
        async with self._operation(config["configurable"]["thread_id"], "aput"):
            committed = await super().aput(config, checkpoint, metadata, new_versions)
        # The audited saver TX, original connection and lock have all exited.
        if not committed["configurable"].get("checkpoint_ns") and self.after_root_commit is not None:
            await self.after_root_commit(committed, metadata)
        return committed

    async def aput_writes(self, config, writes, task_id, task_path=""):
        async with self._operation(config["configurable"]["thread_id"], "aput_writes"):
            return await super().aput_writes(config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id):
        async with self._operation(thread_id, "adelete_thread"):
            return await super().adelete_thread(thread_id)

    @asynccontextmanager
    async def _cursor(self, *, pipeline=False):
        operation = self._mutation.get()
        if operation is None:
            async with super()._cursor(pipeline=pipeline) as cur:
                yield cur
            return
        async with self.lock, _ainternal.get_connection(self.conn) as conn:
            async with conn.transaction(), conn.cursor(binary=True, row_factory=dict_row) as cur:
                await self._write_fence.validate(cur, thread_id=operation[0], operation=operation[1])
                yield cur
