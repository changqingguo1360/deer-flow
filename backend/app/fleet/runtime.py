"""Separate new-job tool admission from accepted-work driver availability."""

from deerflow.mcp.tasks.fleet_runtime import set_fleet_job_submitter


def fleet_runtime(app):
    services = getattr(getattr(app.state, "extensions", None), "services", ())
    candidates = [service for _, service in services if getattr(service, "fleet_protocol_version", None) == 1]
    if len(candidates) > 1:
        raise RuntimeError("Exactly one Fleet runtime is permitted")
    return candidates[0] if candidates else None


def validate_fleet_task_runtime(app, *, enabled, repository_available):
    runtime = fleet_runtime(app)
    if runtime is not None and runtime.config.enabled:
        if not runtime.ready or not enabled or not repository_available:
            raise RuntimeError("Enabled Fleet requires a ready runtime, mcp_tasks.enabled and durable SQL task tracking")


def install_fleet_tools(app, submitter):
    set_fleet_job_submitter(None)
    runtime = fleet_runtime(app)
    if runtime is None or not runtime.config.jobs_enabled:
        return
    if not runtime.ready or runtime.jobs is None or submitter is None:
        raise RuntimeError("Fleet submission requires its ready tracking driver and task service")
    names = tuple(sorted(name for name, profile in runtime.config.profiles.items() if profile.kind == "job"))
    set_fleet_job_submitter(submitter, profile_names=names, scheduled_job_slots=runtime.config.scheduled_job_slots)


def validate_fleet_plugin_configuration(plugins):
    """Validate the canonical Fleet startup contract without importing it."""
    for plugin in plugins:
        if plugin.use != "deerflow_ecs_fleet:install" or not plugin.enabled or not plugin.config.get("enabled", False):
            continue
        if not plugin.required or plugin.table_prefix != "fleet_":
            raise RuntimeError("Enabled Fleet requires required: true and table_prefix: fleet_")
        if not plugin.config.get("nas_root") or not plugin.config.get("nas_identity"):
            raise RuntimeError("Enabled Gateway Fleet requires NAS root and identity for accepted work")
