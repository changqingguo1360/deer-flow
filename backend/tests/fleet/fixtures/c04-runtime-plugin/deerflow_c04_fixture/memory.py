"""Configured PG MemoryManager acceptance adapter; no default memory product."""

import json
import threading
from contextlib import contextmanager
from contextvars import copy_context
from typing import ClassVar

from pydantic import PrivateAttr
from sqlalchemy import text

from deerflow.agents.memory.manager import MemoryManager


class PostgresMemory(MemoryManager):
    supports_search: ClassVar[bool] = True
    remote_mutation_mode: ClassVar[str] = "transactional"
    _sf: object = PrivateAttr()
    _table: str = PrivateAttr(default="c06_memory")
    _transactions: object = PrivateAttr(default=None)
    _extract: object = PrivateAttr(default=None)
    _pending: dict = PrivateAttr(default_factory=dict)
    _lock: object = PrivateAttr(default_factory=threading.RLock)
    _threads: list = PrivateAttr(default_factory=list)
    _failures: list = PrivateAttr(default_factory=list)

    @classmethod
    def from_config(cls, backend_config, *, mode="middleware", **host_hooks):
        manager = cls(backend_config=backend_config, mode=mode)
        manager._sf = host_hooks["session_factory"]
        manager._table = host_hooks.get("memory_table", "c06_memory")
        if manager._table not in {"c06_memory", "c06_local_memory"}:
            raise ValueError("Unapproved fixture memory table")
        manager._transactions = host_hooks.get("mutation_transactions")
        manager._extract = host_hooks.get("memory_extractor", lambda messages: str(messages[-1]))
        return manager

    @contextmanager
    def _write(self, user_id, thread_id=None):
        if self._transactions is not None:
            with self._transactions.sync(user_id=user_id, thread_id=thread_id) as session:
                yield session
        else:
            with self._sf() as session, session.begin():
                yield session

    def add(self, thread_id, messages, *, agent_name=None, user_id=None, trace_id=None):
        context = copy_context()
        authority = self._transactions.capture(user_id=user_id, thread_id=thread_id) if self._transactions is not None else None
        key = (user_id, thread_id, agent_name, authority)
        with self._lock:
            self._pending[key] = (context, self._transactions, list(messages))

    def _drain(self):
        with self._lock:
            pending = list(self._pending.items())
            self._pending.clear()
        for (user, thread, agent, _), (context, transactions, messages) in pending:

            def apply():
                result = self._extract(messages)
                writer = transactions.sync(user_id=user, thread_id=thread) if transactions is not None else self._write(user, thread)
                with writer as session:
                    session.execute(
                        text("INSERT INTO " + self._table + "(user_id,agent_name,fact_id,content) VALUES (:u,:a,:id,:c) ON CONFLICT(user_id,agent_name,fact_id) DO UPDATE SET content=excluded.content"),
                        {"u": user, "a": agent or "__default__", "id": thread, "c": str(result)},
                    )

            try:
                context.run(apply)
            except BaseException as exc:
                self._failures.append(exc)

    def add_nowait(self, thread_id, messages, *, agent_name=None, user_id=None):
        self.add(thread_id, messages, agent_name=agent_name, user_id=user_id)
        worker = threading.Thread(target=self._drain)
        self._threads.append(worker)
        worker.start()

    def shutdown_flush(self, timeout):
        import time

        deadline = time.monotonic() + timeout
        with self._lock:
            if self._pending:
                worker = threading.Thread(target=self._drain)
                self._threads.append(worker)
                worker.start()
        for worker in self._threads:
            worker.join(max(0, deadline - time.monotonic()))
            if worker.is_alive():
                return False
        if self._failures:
            raise self._failures[0]
        return True

    def get_context(self, user_id, *, agent_name=None, thread_id=None):
        if self._transactions is not None:
            self._transactions.capture(user_id=user_id, thread_id=thread_id)
        return json.dumps(self.get_memory(user_id=user_id, agent_name=agent_name))

    def get_memory(self, *, user_id=None, agent_name=None):
        if self._transactions is not None:
            self._transactions.capture(user_id=user_id)
        with self._sf() as session:
            rows = session.execute(text("SELECT fact_id,content FROM " + self._table + " WHERE user_id=:u AND agent_name=:a ORDER BY fact_id"), {"u": user_id, "a": agent_name or "__default__"}).mappings()
            return {"facts": [dict(row) for row in rows]}

    def search(self, query, top_k=5, *, user_id=None, agent_name=None, category=None):
        return [row for row in self.get_memory(user_id=user_id, agent_name=agent_name)["facts"] if query in row["content"]][:top_k]

    def create_fact(self, content, category="general", confidence=1.0, *, user_id=None, agent_name=None):
        import hashlib

        fact_id = hashlib.sha256(content.encode()).hexdigest()
        with self._write(user_id) as session:
            session.execute(text("INSERT INTO " + self._table + "(user_id,agent_name,fact_id,content) VALUES (:u,:a,:id,:c)"), {"u": user_id, "a": agent_name or "__default__", "id": fact_id, "c": content})
        return self.get_memory(user_id=user_id, agent_name=agent_name), fact_id

    def update_fact(self, fact_id, content, *, user_id=None, agent_name=None, **kwargs):
        with self._write(user_id) as session:
            session.execute(text("UPDATE " + self._table + " SET content=:c WHERE user_id=:u AND agent_name=:a AND fact_id=:id"), {"u": user_id, "a": agent_name or "__default__", "id": fact_id, "c": content})
        return {"id": fact_id, "content": content}

    def delete_fact(self, fact_id, *, user_id=None, agent_name=None):
        with self._write(user_id) as session:
            session.execute(text("DELETE FROM " + self._table + " WHERE user_id=:u AND agent_name=:a AND fact_id=:id"), {"u": user_id, "a": agent_name or "__default__", "id": fact_id})
