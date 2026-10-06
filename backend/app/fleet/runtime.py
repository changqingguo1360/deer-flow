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


def validate_fleet_plugin_configuration(plugins, *, host_config=None):
    """Validate the canonical Fleet startup contract without importing it."""
    for plugin in plugins:
        if plugin.use != "deerflow_ecs_fleet:install" or not plugin.enabled or not plugin.config.get("enabled", False):
            continue
        if not plugin.required or plugin.table_prefix != "fleet_":
            raise RuntimeError("Enabled Fleet requires required: true and table_prefix: fleet_")
        if not plugin.config.get("nas_root") or not plugin.config.get("nas_identity"):
            raise RuntimeError("Enabled Gateway Fleet requires NAS root and identity for accepted work")
        if plugin.config.get("agents_enabled", False):
            if host_config is None:
                raise RuntimeError("Remote Agent requires actual host persistence configuration")
            validate_remote_agent_host_configuration(host_config)


def validate_remote_agent_shared_storage(config):
    """Require the actual checkpoint and Store resolvers to share application PG."""
    from psycopg.conninfo import conninfo_to_dict

    from deerflow.persistence.postgres_schema import normalize_libpq_dsn
    from deerflow.runtime.checkpointer.provider import _resolve_checkpointer_config
    from deerflow.runtime.store.provider import _resolve_store_config

    database = config.database
    checkpoint = _resolve_checkpointer_config(config)
    store = _resolve_store_config(config)
    if store != checkpoint:
        raise RuntimeError("Remote Agent requires matching checkpoint and Store configuration")
    if database.backend != "postgres" or checkpoint.type != "postgres" or not database.postgres_url or not checkpoint.connection_string:
        raise RuntimeError("Remote Agent requires shared PostgreSQL application and checkpoint storage")
    try:
        application = conninfo_to_dict(normalize_libpq_dsn(database.postgres_url))
        checkpoints = conninfo_to_dict(normalize_libpq_dsn(checkpoint.connection_string))
    except Exception:
        raise RuntimeError("Remote Agent requires valid shared PostgreSQL configuration") from None
    keys = ("host", "hostaddr", "port", "dbname", "user", "service", "options")
    if any(application.get(key, "5432" if key == "port" else "") != checkpoints.get(key, "5432" if key == "port" else "") for key in keys) or checkpoint.postgres_schema != database.postgres_schema:
        raise RuntimeError("Remote Agent requires matching PostgreSQL checkpoint/application identity and schema")


def validate_remote_agent_host_configuration(config):
    """Use actual storage selection plus the Gateway ownership readiness guard."""
    validate_remote_agent_shared_storage(config)
    if config.run_events.backend != "db" or not config.run_ownership.heartbeat_enabled:
        raise RuntimeError("Remote Agent requires database run events and ownership heartbeat")


def validate_remote_agent_runtime(app, session_factory):
    """Admit C only after the normal startup has installed its participants."""
    runtime = fleet_runtime(app)
    if runtime is None or not runtime.config.enabled or not runtime.config.agents_enabled:
        return
    from app.fleet.events import FleetGatewayBridge
    from app.fleet.execution import BoundFleetRunBackend
    from app.fleet.ownership import FleetRunOwnership
    from deerflow.runtime.events.store.db import DbRunEventStore

    manager = getattr(app.state, "run_manager", None)
    heartbeat = getattr(manager, "_heartbeat_task", None)
    if (
        not runtime.ready
        or session_factory is None
        or runtime.session_factory is not session_factory
        or not isinstance(getattr(app.state, "fleet_ownership", None), FleetRunOwnership)
        or not isinstance(getattr(app.state, "bound_run_execution_backend", None), BoundFleetRunBackend)
        or not isinstance(getattr(app.state, "stream_bridge", None), FleetGatewayBridge)
        or not isinstance(getattr(app.state, "run_event_store", None), DbRunEventStore)
        or getattr(app.state, "checkpointer", None) is None
        or getattr(app.state, "store", None) is None
        or any(getattr(app.state, name, None) is None for name in ("fleet_workspaces", "fleet_workspace_files", "fleet_scheduler_tickets"))
        or manager is None
        or not manager.heartbeat_enabled
        or heartbeat is None
        or heartbeat.done()
    ):
        raise RuntimeError("Remote Agent requires ready Fleet participants, shared SQL persistence and an active ownership heartbeat")
