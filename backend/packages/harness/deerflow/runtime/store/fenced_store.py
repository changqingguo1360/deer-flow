"""Audited same-transaction PostgreSQL Store mutation adapter."""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from importlib.metadata import version

from langgraph.checkpoint.postgres import _ainternal
from langgraph.store.base import BaseStore, GetOp, ListNamespacesOp, PutOp, SearchOp
from langgraph.store.postgres.aio import AsyncPostgresStore
from langgraph.store.postgres.base import _get_index_params, _get_vector_type_ops, _namespace_to_text
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from deerflow.runtime.execution.mutation_context import MutationTarget, OwnershipRejected, RemoteMutationContext, current_remote_mutation_context


class _PreparedEmbeddings:
    def __init__(self, delegate):
        self.delegate = delegate
        self.prepared = ContextVar("store_prepared_embeddings", default=None)

    async def aembed_documents(self, texts):
        prepared = self.prepared.get()
        if prepared is None or tuple(texts) not in prepared:
            raise OwnershipRejected("Unprepared Store embedding request")
        return prepared[tuple(texts)]


class FencedAsyncPostgresStore(AsyncPostgresStore):
    # Direct conveniences retain the caller context rather than sending an Op
    # to the constructor-created background batching task without its context.
    aget = BaseStore.aget
    asearch = BaseStore.asearch
    aput = BaseStore.aput
    adelete = BaseStore.adelete
    alist_namespaces = BaseStore.alist_namespaces

    def __init__(self, conn, *, mutation_capability, schema="", **kwargs):
        if not isinstance(getattr(mutation_capability, "context", None), RemoteMutationContext):
            raise OwnershipRejected("Remote Store requires an original execution capability")
        if version("langgraph-checkpoint-postgres") != "3.1.1":
            raise RuntimeError("Unsupported fenced PostgreSQL Store version")
        if kwargs.get("pipe") is not None:
            raise OwnershipRejected("Remote Store requires explicit transactions without a pipeline")
        super().__init__(conn, **kwargs)
        self._task.cancel()
        self._task = None
        self._mutation_capability = mutation_capability
        self._schema = schema
        self._operation = ContextVar("store_operation", default=None)
        if self.embeddings is not None:
            self.embeddings = _PreparedEmbeddings(self.embeddings)

    @classmethod
    @asynccontextmanager
    async def from_conn_string(cls, conn_string, *, mutation_capability, schema="", pipeline=False, pool_config=None, index=None, ttl=None):
        # Pipeline performance mode cannot replace the atomic remote transaction.
        if pipeline:
            raise OwnershipRejected("Remote Store pipeline construction is unsupported")
        if pool_config is not None:
            config = pool_config.copy()
            async with AsyncConnectionPool(
                conn_string, min_size=config.pop("min_size", 1), max_size=config.pop("max_size", None), kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row, **(config.pop("kwargs", None) or {})}, **config
            ) as pool:
                yield cls(pool, mutation_capability=mutation_capability, schema=schema, index=index, ttl=ttl)
        else:
            async with await AsyncConnection.connect(conn_string, autocommit=True, prepare_threshold=0, row_factory=dict_row) as conn:
                yield cls(conn, mutation_capability=mutation_capability, schema=schema, index=index, ttl=ttl)

    async def abatch(self, ops):
        operations = list(ops)
        if any(type(op) not in {GetOp, SearchOp, PutOp, ListNamespacesOp} for op in operations):
            raise OwnershipRejected("Unsupported remote Store operation")
        writing = any(isinstance(op, PutOp) or isinstance(op, (GetOp, SearchOp)) and op.refresh_ttl for op in operations)
        context = current_remote_mutation_context()
        if writing and context != self._mutation_capability.context:
            raise OwnershipRejected("Remote Store mutation context rejected")
        embedding_token = None
        if self.embeddings is not None:
            requests = []
            puts = [(i, op) for i, op in enumerate(operations) if isinstance(op, PutOp)]
            searches = [(i, op) for i, op in enumerate(operations) if isinstance(op, SearchOp)]
            if puts:
                _, request = self._prepare_batch_PUT_queries(puts)
                if request:
                    requests.append(tuple(param[-1] for param in request[1]))
            if searches:
                _, request = self._prepare_batch_search_queries(searches)
                if request:
                    requests.append(tuple(query for _, query in request))
            prepared = {}
            # External work finishes before execution locks or the SQL TX.
            # The original authority is validated freshly at actual SQL entry.
            for texts in requests:
                if texts not in prepared:
                    prepared[texts] = await self.embeddings.delegate.aembed_documents(list(texts))
            embedding_token = self.embeddings.prepared.set(prepared)
        token = self._operation.set((writing, context))
        try:
            return await super().abatch(operations)
        finally:
            self._operation.reset(token)
            if embedding_token is not None:
                self.embeddings.prepared.reset(embedding_token)

    def _get_batch_GET_ops_queries(self, get_ops):
        refresh = [(idx, op) for idx, op in get_ops if op.refresh_ttl]
        queries = super()._get_batch_GET_ops_queries(refresh) if refresh else []
        groups = {}
        for idx, op in get_ops:
            if not op.refresh_ttl:
                groups.setdefault(op.namespace, []).append((idx, op.key))
        for namespace, items in groups.items():
            expiry = " AND (expires_at IS NULL OR expires_at > NOW())" if self._omit_expired else ""
            queries.append(("SELECT key,value,created_at,updated_at FROM store WHERE prefix=%s AND key=ANY(%s)" + expiry, (_namespace_to_text(namespace), [key for _, key in items]), namespace, items))
        return queries

    @asynccontextmanager
    async def _cursor(self, *, pipeline=False):
        operation = self._operation.get()
        async with self.lock, _ainternal.get_connection(self.conn) as conn:
            async with conn.transaction(), conn.cursor(binary=True, row_factory=dict_row) as cur:
                if operation is None or not operation[0]:
                    await cur.execute("SET TRANSACTION READ ONLY")
                else:
                    await self._mutation_capability.validate_cursor(
                        cur,
                        context=operation[1],
                        operation="store.write",
                        targets=(MutationTarget(run_id=self._mutation_capability.context.run_id, thread_id=self._mutation_capability.context.thread_id, user_id=self._mutation_capability.context.user_id),),
                    )
                yield cur
                if operation is not None and operation[0]:
                    await self._mutation_capability.validate_cursor(
                        cur,
                        context=operation[1],
                        operation="store.write",
                        targets=(MutationTarget(run_id=self._mutation_capability.context.run_id, thread_id=self._mutation_capability.context.thread_id, user_id=self._mutation_capability.context.user_id),),
                    )

    async def setup(self):
        required = {"store_migrations": {"v"}, "store": {"prefix", "key", "value", "created_at", "updated_at", "expires_at", "ttl_minutes"}}
        if self.index_config:
            required.update(vector_migrations={"v"}, store_vectors={"prefix", "key", "field_name", "embedding", "created_at", "updated_at"})
        async with self._cursor() as cur:
            await cur.execute("SELECT current_schema() AS schema")
            schema = (await cur.fetchone())["schema"]
            if not schema or self._schema and schema != self._schema:
                raise RuntimeError("Store schema is not ready")
            await cur.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=ANY(%s)", (schema, list(required)))
            found = {}
            for row in await cur.fetchall():
                found.setdefault(row["table_name"], set()).add(row["column_name"])
            if any(not columns <= found.get(table, set()) for table, columns in required.items()):
                raise RuntimeError("Store schema is not ready")
            expected_types = {
                "store_migrations": {"v": ("integer", True)},
                "store": {
                    "prefix": ("text", True),
                    "key": ("text", True),
                    "value": ("jsonb", True),
                    "created_at": ("timestamp with time zone", False),
                    "updated_at": ("timestamp with time zone", False),
                    "expires_at": ("timestamp with time zone", False),
                    "ttl_minutes": ("integer", False),
                },
            }
            if self.index_config:
                vector_type = self.index_config.get("ann_index_config", {}).get("vector_type", "vector")
                expected_types.update(
                    vector_migrations={"v": ("integer", True)},
                    store_vectors={
                        "prefix": ("text", True),
                        "key": ("text", True),
                        "field_name": ("text", True),
                        "embedding": (f"{vector_type}({self.index_config['dims']})", False),
                        "created_at": ("timestamp with time zone", False),
                        "updated_at": ("timestamp with time zone", False),
                    },
                )
            await cur.execute(
                (
                    "SELECT c.relname AS name,a.attname AS column,format_type(a.atttypid,a.atttypmod) AS type,a.attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN "
                    "pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=ANY(%s) AND a.attnum>0 AND NOT a.attisdropped"
                ),
                (schema, list(required)),
            )
            actual_types = {(row["name"], row["column"]): (row["type"], row["attnotnull"]) for row in await cur.fetchall()}
            if any(actual_types.get((table, column)) != shape for table, columns in expected_types.items() for column, shape in columns.items()):
                raise RuntimeError("Store column types are not ready")
            for table, versions in (("store_migrations", set(range(len(self.MIGRATIONS)))), *(([("vector_migrations", set(range(len(self.VECTOR_MIGRATIONS))))]) if self.index_config else [])):
                await cur.execute(sql.SQL("SELECT v FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
                if {row["v"] for row in await cur.fetchall()} != versions:
                    raise RuntimeError("Store migrations are not ready")
            await cur.execute(
                (
                    "SELECT c.relname AS name,t.relname AS table_name,i.indisvalid,i.indisready,i.indisprimary,i.indisunique,pg_get_expr(i.indpred,i.indrelid) AS "
                    "predicate,c.reloptions,ARRAY(SELECT op.opcname FROM unnest(i.indclass) WITH ORDINALITY k(opid,ordinality) JOIN pg_opclass op ON op.oid=k.opid ORDER BY k.ordinality) AS "
                    "opclasses,am.amname,pg_get_indexdef(c.oid) AS definition,ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(attnum,ordinality) JOIN pg_attribute a ON "
                    "a.attrelid=t.oid AND a.attnum=k.attnum ORDER BY k.ordinality) AS columns FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_class t ON t.oid=i.indrelid JOIN "
                    "pg_namespace n ON n.oid=t.relnamespace JOIN pg_am am ON am.oid=c.relam WHERE n.nspname=%s"
                ),
                (schema,),
            )
            indexes = {row["name"]: row for row in await cur.fetchall() if row["indisvalid"] and row["indisready"]}
            expected_indexes = {
                "store_pkey": ("store", ["prefix", "key"], True, "btree"),
                "store_migrations_pkey": ("store_migrations", ["v"], True, "btree"),
                "store_prefix_idx": ("store", ["prefix"], False, "btree"),
                "idx_store_expires_at": ("store", ["expires_at"], False, "btree"),
            }
            if self.index_config:
                expected_indexes.update(store_vectors_pkey=("store_vectors", ["prefix", "key", "field_name"], True, "btree"), vector_migrations_pkey=("vector_migrations", ["v"], True, "btree"))
                index_type, index_params = _get_index_params(self)
                if index_type != "flat":
                    expected_indexes["store_vectors_embedding_idx"] = ("store_vectors", ["embedding"], False, index_type)
            for name, shape in expected_indexes.items():
                row = indexes.get(name)
                if row is None or (row["table_name"], row["columns"], row["indisprimary"], row["amname"]) != shape or row["indisunique"] != shape[2] or name != "idx_store_expires_at" and row["predicate"] is not None:
                    raise RuntimeError("Store indexes are not ready")
            if "text_pattern_ops" not in indexes["store_prefix_idx"]["definition"] or "WHERE (expires_at IS NOT NULL)" not in indexes["idx_store_expires_at"]["definition"]:
                raise RuntimeError("Store index definitions are not ready")
            if self.index_config:
                if index_type != "flat":
                    embedding_index = indexes["store_vectors_embedding_idx"]
                    if embedding_index["opclasses"] != [_get_vector_type_ops(self)] or set(embedding_index["reloptions"] or []) != {f"{key}={value}" for key, value in index_params.items()}:
                        raise RuntimeError("Store vector distance/index configuration is not ready")
                await cur.execute(
                    "SELECT con.convalidated,con.confdeltype,con.confrelid = to_regclass(%s) AS correct_target,pg_get_constraintdef(con.oid) AS definition FROM pg_constraint con WHERE con.conrelid=to_regclass(%s) AND con.contype='f'",
                    (schema + ".store", schema + ".store_vectors"),
                )
                if not any(
                    row["convalidated"] and row["confdeltype"] == "c" and row["correct_target"] and "FOREIGN KEY (prefix, key) REFERENCES" in row["definition"] and "(prefix, key) ON DELETE CASCADE" in row["definition"]
                    for row in await cur.fetchall()
                ):
                    raise RuntimeError("Store vector ownership constraint is not ready")

    async def sweep_ttl(self):
        raise OwnershipRejected("Global Store sweep is not authorized for an execution")

    async def start_ttl_sweeper(self, *args, **kwargs):
        raise OwnershipRejected("Global Store sweep is not authorized for an execution")
