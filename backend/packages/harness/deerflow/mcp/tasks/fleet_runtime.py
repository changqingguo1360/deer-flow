"""Host-installed Fleet submit boundary; no optional extension or app imports."""

from contextlib import contextmanager
from contextvars import ContextVar

from deerflow.runtime.execution.mutation_context import OwnershipRejected, current_remote_mutation_context

from .runtime import McpTaskConfigurationError, McpTaskSubmitter

_submitter: McpTaskSubmitter | None = None
_profile_names: tuple[str, ...] = ()
_scheduled_job_slots: dict[str, str] = {}
_scoped_fleet_runtime = ContextVar("private_fleet_job_runtime", default=None)


def set_fleet_job_submitter(submitter: McpTaskSubmitter | None, *, profile_names: tuple[str, ...] = (), scheduled_job_slots: dict[str, str] | None = None) -> None:
    global _submitter, _profile_names, _scheduled_job_slots
    _submitter = submitter
    _profile_names = profile_names if submitter is not None else ()
    _scheduled_job_slots = dict(scheduled_job_slots or {}) if submitter is not None else {}


def is_fleet_job_runtime_available() -> bool:
    if _scoped_fleet_runtime.get() is not None:
        get_fleet_job_submitter()
        return True
    return current_remote_mutation_context() is None and _submitter is not None


def get_fleet_job_submitter() -> McpTaskSubmitter:
    scoped = _scoped_fleet_runtime.get()
    if scoped is not None:
        submitter, capability, _, _ = scoped
        if current_remote_mutation_context() != capability.context:
            raise OwnershipRejected("Remote Fleet submission lost original context")
        return submitter
    if current_remote_mutation_context() is not None:
        raise OwnershipRejected("Remote Fleet submission requires private bound service")
    if _submitter is None:
        raise McpTaskConfigurationError("Fleet job submission is not enabled in this Gateway")
    return _submitter


def get_fleet_job_profile_names() -> tuple[str, ...]:
    """Expose names only; container image, network and credentials stay host-owned."""
    scoped = _scoped_fleet_runtime.get()
    if scoped is not None:
        get_fleet_job_submitter()
        return scoped[2]
    if current_remote_mutation_context() is not None:
        return ()
    return _profile_names


def get_fleet_scheduled_job_slots() -> dict[str, str]:
    """Return only operator-approved named slot/profile bindings."""
    scoped = _scoped_fleet_runtime.get()
    if scoped is not None:
        get_fleet_job_submitter()
        return dict(scoped[3])
    if current_remote_mutation_context() is not None:
        return {}
    return dict(_scheduled_job_slots)


@contextmanager
def fleet_job_submitter_scope(submitter, *, profile_names=(), scheduled_job_slots=None):
    capability = getattr(submitter._repository, "_mutation_capability", None)
    driver = submitter.drivers.get("fleet")
    parent = getattr(getattr(driver, "jobs", None), "parent_capability", None)
    if (
        capability is None
        or parent is None
        or current_remote_mutation_context() != capability.context
        or (parent.owner.parent_run_id, parent.owner.agent_task_id, parent.owner.generation, parent.owner.user_id, parent.owner.thread_id)
        != (capability.context.run_id, capability.context.agent_task_id, capability.context.generation, capability.context.user_id, capability.context.thread_id)
    ):
        raise OwnershipRejected("Remote Fleet scope requires original bound driver and tracking service")
    token = _scoped_fleet_runtime.set((submitter, capability, tuple(profile_names), dict(scheduled_job_slots or {})))
    try:
        yield
    finally:
        _scoped_fleet_runtime.reset(token)
