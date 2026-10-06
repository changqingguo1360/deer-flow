"""Optional opaque model-call authority; the harness knows no host ledger."""

from contextlib import contextmanager
from contextvars import ContextVar

_call_budget = ContextVar("private_model_call_budget", default=None)


def current_call_budget():
    return _call_budget.get()


@contextmanager
def call_budget_scope(capability):
    token = _call_budget.set(capability)
    try:
        yield
    finally:
        _call_budget.reset(token)


def mark_private_call_denial(error):
    """Carry opaque identity across SDK and model-created asyncio tasks."""
    capability = current_call_budget()
    if capability is not None:
        error._deerflow_call_budget = capability


def is_private_call_denial(error):
    capability = current_call_budget()
    return capability is not None and getattr(error, "_deerflow_call_budget", None) is capability
