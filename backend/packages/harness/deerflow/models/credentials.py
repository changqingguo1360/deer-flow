"""Trusted, non-JSON provider credentials scoped to one execution context."""

from collections.abc import Callable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

AUTH_FIELDS = frozenset({"api_key", "openai_api_key", "anthropic_api_key", "google_api_key", "aws_access_key_id", "aws_secret_access_key", "aws_session_token", "azure_ad_token"})
_resolver: ContextVar[Callable | None] = ContextVar("deerflow_model_credential_resolver", default=None)


@contextmanager
def model_credential_scope(resolver):
    if not callable(resolver):
        raise TypeError("Trusted model credential resolver required")
    token = _resolver.set(resolver)
    try:
        yield
    finally:
        _resolver.reset(token)


def resolve_model_credentials(name, use):
    resolver = _resolver.get()
    if resolver is None:
        return None
    values = resolver(name, use)
    if not isinstance(values, Mapping) or not set(values) <= AUTH_FIELDS:
        raise ValueError("Unsupported scoped model authentication fields")
    if any(not isinstance(v, str) or not v for v in values.values()):
        raise ValueError("Scoped model credentials must be nonempty strings")
    return dict(values)
