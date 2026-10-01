"""Host-installed Fleet submit boundary; no optional extension or app imports."""

from .runtime import McpTaskConfigurationError, McpTaskSubmitter

_submitter: McpTaskSubmitter | None = None
_profile_names: tuple[str, ...] = ()


def set_fleet_job_submitter(submitter: McpTaskSubmitter | None, *, profile_names: tuple[str, ...] = ()) -> None:
    global _submitter, _profile_names
    _submitter = submitter
    _profile_names = profile_names if submitter is not None else ()


def is_fleet_job_runtime_available() -> bool:
    return _submitter is not None


def get_fleet_job_submitter() -> McpTaskSubmitter:
    if _submitter is None:
        raise McpTaskConfigurationError("Fleet job submission is not enabled in this Gateway")
    return _submitter


def get_fleet_job_profile_names() -> tuple[str, ...]:
    """Expose names only; container image, network and credentials stay host-owned."""
    return _profile_names
