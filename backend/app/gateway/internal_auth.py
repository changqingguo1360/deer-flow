"""Authentication for trusted Gateway internal callers."""

from __future__ import annotations

import os
import secrets
from types import SimpleNamespace
from typing import Any

from deerflow.config.paths import make_safe_user_id
from deerflow.runtime.user_context import DEFAULT_USER_ID

INTERNAL_AUTH_HEADER_NAME = "X-DeerFlow-Internal-Token"
INTERNAL_OWNER_USER_ID_HEADER_NAME = "X-DeerFlow-Owner-User-Id"
INTERNAL_AUTH_ENV_VAR = "DEER_FLOW_INTERNAL_AUTH_TOKEN"
INTERNAL_SYSTEM_ROLE = "internal"


def _load_internal_auth_token() -> str:
    token = os.environ.get(INTERNAL_AUTH_ENV_VAR)
    if token:
        return token
    return secrets.token_urlsafe(32)


_INTERNAL_AUTH_TOKEN = _load_internal_auth_token()


def create_internal_auth_headers(*, owner_user_id: str | None = None) -> dict[str, str]:
    """Return headers that authenticate trusted Gateway internal calls."""
    headers = {INTERNAL_AUTH_HEADER_NAME: _INTERNAL_AUTH_TOKEN}
    if owner_user_id:
        headers[INTERNAL_OWNER_USER_ID_HEADER_NAME] = owner_user_id
    return headers


def is_valid_internal_auth_token(token: str | None) -> bool:
    """Return True when *token* matches this Gateway worker's internal token."""
    return bool(token) and secrets.compare_digest(token, _INTERNAL_AUTH_TOKEN)


def get_internal_user(owner_user_id: str | None = None):
    """Return the synthetic user used for trusted internal channel calls.

    When *owner_user_id* is provided (extracted from the
    ``X-DeerFlow-Owner-User-Id`` header), the synthetic user's ``.id``
    carries the actual channel owner instead of ``DEFAULT_USER_ID``.
    This ensures that ``get_effective_user_id()`` and downstream
    filesystem-path resolution (per-user custom skills, memory, thread
    data) use the correct identity for IM channel messages instead of
    falling back to ``"default"``.

    The owner id is normalized through :func:`make_safe_user_id` so that
    IM channel ids containing characters outside ``[A-Za-z0-9_-]`` (e.g.
    Feishu ``open_id`` prefixed with ``ou_`` and containing underscores
    that the rest of the system may treat as path separators, or
    Telegram chat ids like ``-1001234567890``) cannot be used to escape
    the per-user storage bucket or impersonate a different user via
    header value tricks (e.g. trailing slashes, ``..`` segments). The
    normalization is lossy but deterministic: two distinct raw inputs
    never share a safe id, so cross-user bleed is impossible.
    """
    if owner_user_id:
        effective_id = make_safe_user_id(owner_user_id)
    else:
        effective_id = DEFAULT_USER_ID
    return SimpleNamespace(id=effective_id, system_role=INTERNAL_SYSTEM_ROLE)


def get_trusted_internal_owner_user_id(request: Any) -> str | None:
    """Return the owner override for a trusted internal request, if present.

    The header is ignored for normal browser/API callers. It is only honored
    after ``AuthMiddleware`` has validated the internal auth token and stamped
    the synthetic internal user onto ``request.state.user``.
    """
    user = getattr(getattr(request, "state", None), "user", None)
    if getattr(user, "system_role", None) != INTERNAL_SYSTEM_ROLE:
        return None

    owner_user_id = request.headers.get(INTERNAL_OWNER_USER_ID_HEADER_NAME)
    if not owner_user_id:
        return None
    owner_user_id = owner_user_id.strip()
    return owner_user_id or None


# Separate from the internal token: possessing that token cannot sign a human
# event. This worker-local secret deliberately fails closed across restart/workers.
_HUMAN_EVENT_SECRET = secrets.token_bytes(32)
_HUMAN_EVENT_HEADER = "X-DeerFlow-Human-Event"


def _human_event_payload(owner, thread_id, graph_input, key, expiry):
    import hashlib
    import json

    digest = hashlib.sha256(json.dumps(graph_input, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    return json.dumps([make_safe_user_id(owner), thread_id, digest, key, expiry], separators=(",", ":")).encode()


def create_channel_human_headers(*, owner_user_id, thread_id, graph_input, event_key):
    """Only the three real ChannelManager human launch sites call this signer."""
    import hashlib
    import hmac
    import time

    key = "channel:" + hashlib.sha256(event_key.encode()).hexdigest()
    expiry = int(time.time()) + 300
    payload = _human_event_payload(owner_user_id, thread_id, graph_input, key, expiry)
    signature = hmac.new(_HUMAN_EVENT_SECRET, payload, hashlib.sha256).hexdigest()
    return dict(create_internal_auth_headers(owner_user_id=owner_user_id), **{"Idempotency-Key": key, _HUMAN_EVENT_HEADER: str(expiry) + "." + signature})


def authenticated_channel_human_event(request, thread_id, graph_input):
    """Return signed key/freshness; expired exact receipt reuse grants no new intent."""
    import hashlib
    import hmac
    import time

    from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL

    if getattr(request.state, "auth_source", None) != AUTH_SOURCE_INTERNAL:
        return None
    owner = get_trusted_internal_owner_user_id(request)
    key = request.headers.get("Idempotency-Key")
    header = request.headers.get(_HUMAN_EVENT_HEADER, "")
    if not owner or not key or len(key) > 128:
        return None
    try:
        expiry_text, signature = header.split(".", 1)
        expiry = int(expiry_text)
    except (ValueError, TypeError):
        return None
    expected = hmac.new(_HUMAN_EVENT_SECRET, _human_event_payload(owner, thread_id, graph_input, key, expiry), hashlib.sha256).hexdigest()
    if not secrets.compare_digest(signature, expected):
        return None
    return key, int(time.time()) <= expiry
