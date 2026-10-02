"""Trusted non-JSON definition-store scope; Local configuration stays unchanged."""

from contextlib import contextmanager
from contextvars import ContextVar

_stores = ContextVar("deerflow_private_agent_definition_stores", default=None)


@contextmanager
def agent_definition_store_scope(agent_store, managed_subagent_store):
    if agent_store is None or managed_subagent_store is None:
        raise ValueError("Both trusted definition stores required")
    token = _stores.set((agent_store, managed_subagent_store))
    try:
        yield
    finally:
        _stores.reset(token)


def get_scoped_definition_stores():
    return _stores.get()
