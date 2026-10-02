"""Private bounded Agent teardown contract; independent of app and harness."""

import time
from contextlib import contextmanager
from contextvars import ContextVar

TOTAL_CLEANUP_SECONDS = 120.0
CLEANUP_DEADLINE_EXIT_CODE = 70
_deadline_observer = ContextVar("agent_cleanup_deadline_observer", default=None)
_isolated_abort = ContextVar("agent_cleanup_isolated_abort", default=None)


class PendingAgentCleanup(RuntimeError):
    """Trusted host cleanup handle, never graph configuration or public JSON."""


class CleanupBudget:
    def __init__(self):
        self._deadline = None

    def start(self):
        if self._deadline is None:
            self._deadline = time.monotonic() + TOTAL_CLEANUP_SECONDS
            observer = _deadline_observer.get()
            if observer is not None:
                observer(self._deadline)
        return self._deadline

    @property
    def deadline(self):
        return self.start()

    def remaining(self):
        return max(0.0, self.start() - time.monotonic())


@contextmanager
def isolated_cleanup_policy(observer, abort):
    deadline_token = _deadline_observer.set(observer)
    abort_token = _isolated_abort.set(abort)
    try:
        yield
    finally:
        _isolated_abort.reset(abort_token)
        _deadline_observer.reset(deadline_token)


def abort_isolated_cleanup():
    """Only the dedicated bootstrap supplies a physical self-exit callback."""
    callback = _isolated_abort.get()
    if callback is not None:
        callback()
