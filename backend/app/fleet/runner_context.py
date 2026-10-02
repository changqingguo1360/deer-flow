"""Trusted host environment bridge. Private connections never enter tool config."""

import json
from contextlib import AsyncExitStack, ExitStack, contextmanager
from hashlib import sha256
from importlib import metadata
from pathlib import Path

from deerflow_ecs_fleet.launch_spec import Snapshot, WorkerCompatibility, thaw
from deerflow_ecs_fleet.worker.agent_environment import AgentEnvironment

from app.fleet.mutation import FleetCheckpointFence, FleetMutationCapability


def _distribution_files(name):
    distribution = metadata.distribution(name)
    files = {"version": distribution.version}
    for item in distribution.files or ():
        if str(item).endswith((".py", ".so", ".json", ".yaml", ".md")):
            files[str(item)] = sha256(Path(distribution.locate_file(item)).read_bytes()).hexdigest()
    return distribution, files


def runtime_bundle():
    raw = Path("/opt/deerflow/runtime-bundle.json").read_bytes()
    bundle = json.loads(raw)
    if (
        not isinstance(bundle, dict)
        or set(bundle) != {"skills", "plugins", "mcp_servers", "secret_bindings"}
        or not isinstance(bundle["skills"], list)
        or not isinstance(bundle["plugins"], list)
        or not isinstance(bundle["mcp_servers"], dict)
    ):
        raise ValueError("Invalid approved runtime bundle")
    return raw, bundle


def installed_compatibility():
    """Hash actual installed code, provider implementations and bundled snapshots."""
    from importlib.util import find_spec

    files = {}
    for name in ("deer-flow", "deerflow-harness", "deerflow-extension-api", "deerflow-ecs-fleet"):
        _, files[name] = _distribution_files(name)
    binding_bytes = Path("/opt/deerflow/model-bindings.json").read_bytes()
    bindings = json.loads(binding_bytes)
    files["approved-model-bindings"] = sha256(binding_bytes).hexdigest()
    ownership = metadata.packages_distributions()
    for binding in bindings.values():
        use = binding["provider_use"]
        module = use.split(":", 1)[0] if ":" in use else use.rsplit(".", 1)[0]
        installed = find_spec(module)
        if installed is None or installed.origin is None:
            raise ValueError("Approved model provider code is unavailable")
        files["provider-module:" + module] = sha256(Path(installed.origin).read_bytes()).hexdigest()
        for distribution in ownership.get(module.split(".", 1)[0], ()):
            _, files["provider-distribution:" + distribution] = _distribution_files(distribution)
    bundle_bytes, bundle = runtime_bundle()
    files["approved-runtime-bundle"] = sha256(bundle_bytes).hexdigest()
    skill_root = Path("/opt/deerflow/skills").resolve(strict=True)
    skills = []
    declared = set()
    for entry in bundle["skills"]:
        if not isinstance(entry, dict) or set(entry) != {"name", "version", "path"}:
            raise ValueError("Invalid bundled skill binding")
        path = skill_root / entry["path"]
        if path.is_symlink() or not path.resolve(strict=True).is_relative_to(skill_root) or not (path / "SKILL.md").is_file():
            raise ValueError("Bundled skill is missing or escapes runtime")
        from deerflow.skills.parser import parse_skill_file
        from deerflow.skills.types import SkillCategory

        parsed_skill = parse_skill_file(path / "SKILL.md", SkillCategory.PUBLIC)
        if parsed_skill is None or parsed_skill.name != entry["name"]:
            raise ValueError("Bundled skill name differs from actual metadata")
        declared.add(path.resolve())
        content = {}
        for child in sorted(path.rglob("*")):
            if child.is_symlink():
                raise ValueError("Bundled skill symlinks are unsupported")
            if child.is_file():
                content[str(child.relative_to(path))] = sha256(child.read_bytes()).hexdigest()
        digest = "sha256:" + sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        skills.append({"name": entry["name"], "version": entry["version"], "digest": digest})
    if {path.parent.resolve() for path in skill_root.rglob("SKILL.md")} != declared:
        raise ValueError("Runtime contains undeclared skill packages")
    plugins = []
    for entry in bundle["plugins"]:
        if not isinstance(entry, dict) or set(entry) != {"name", "distribution", "use"}:
            raise ValueError("Invalid bundled plugin binding")
        distribution, content = _distribution_files(entry["distribution"])
        digest = "sha256:" + sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        plugins.append({"name": entry["name"], "version": distribution.version, "digest": digest})
        files["plugin-distribution:" + entry["distribution"]] = content
    digest = sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return WorkerCompatibility(runtime_digest="sha256:" + digest, skill_snapshot=Snapshot(entries=skills), plugin_snapshot=Snapshot(entries=plugins))


def control_connection_credentials(private):
    """Read supported connection auth with the same driver parsers as runtime."""
    from urllib.parse import unquote, urlsplit

    from psycopg.conninfo import conninfo_to_dict

    from deerflow.persistence.postgres_schema import normalize_libpq_dsn
    from deerflow.runtime.checkpointer.provider import _resolve_checkpointer_config

    values = set()

    def preserve_raw_url_passwords(raw):
        parsed = urlsplit(raw)
        if parsed.password:
            values.add(parsed.password)
        for entry in parsed.query.split("&"):
            key, separator, value = entry.partition("=")
            if separator and unquote(key) in {"password", "sslpassword"} and value:
                values.add(value)

    checkpoint = _resolve_checkpointer_config(private)
    postgres_sources = {private.database.postgres_url}
    if checkpoint.type == "postgres":
        postgres_sources.add(checkpoint.connection_string)
    try:
        for raw in postgres_sources:
            if raw:
                values.add(raw)
                parsed = conninfo_to_dict(normalize_libpq_dsn(raw))
                values.update(parsed[key] for key in ("password", "sslpassword") if parsed.get(key))
                preserve_raw_url_passwords(raw)
        for raw in (private.stream_bridge.redis_url if private.stream_bridge else None, private.sandbox.ownership.redis_url if private.sandbox.ownership else None):
            if raw:
                values.add(raw)
                from redis.connection import parse_url

                parsed = parse_url(raw)
                if parsed.get("password"):
                    values.add(parsed["password"])
                preserve_raw_url_passwords(raw)
    except Exception:
        # Driver parser errors can contain the raw connection string. Never
        # include those values in the public failure message.
        raise ValueError("Invalid private control connection configuration") from None
    return values


def validate_runtime_configuration(private):
    _, bundle = runtime_bundle()
    actual = {(entry["name"], entry["distribution"], entry["use"]) for entry in bundle["plugins"]}
    configured = {(entry.name, entry.package, entry.use) for entry in private.plugins if entry.enabled}
    if configured != actual:
        raise ValueError("Activated plugins differ from approved runtime bundle")
    if private.skills.path is not None and Path(private.skills.path).resolve() != Path("/opt/deerflow/skills"):
        raise ValueError("Skills must use the approved read-only runtime bundle")
    from urllib.parse import parse_qs, urlsplit

    servers = private.extensions.get_enabled_mcp_servers()
    if set(servers) != set(bundle["mcp_servers"]):
        raise ValueError("Enabled MCP servers differ from approved runtime bundle")
    controls = control_connection_credentials(private)
    forbidden_flags = {"--token", "--password", "--api-key", "--api_key", "--authorization", "--dsn", "--database-url", "--redis-url"}
    for name, server in servers.items():
        bound = bundle["mcp_servers"][name]
        transport = server.type or "stdio"
        if not isinstance(bound, dict) or bound.get("transport") != transport:
            raise ValueError("MCP transport differs from approved runtime")
        if transport in {"http", "sse"}:
            if set(bound) != {"transport", "url"} or server.url != bound["url"]:
                raise ValueError("MCP endpoint differs from approved runtime")
            parsed = urlsplit(server.url)
            if parsed.username is not None or parsed.password is not None or any(part in key.lower() for key in parse_qs(parsed.query) for part in ("token", "key", "secret", "password", "credential")):
                raise ValueError("MCP URL authentication requires private scoped headers")
        elif transport == "stdio":
            if set(bound) != {"transport", "command", "args", "allowed_env_keys"} or (server.command, server.args, set(server.env)) != (bound["command"], bound["args"], set(bound["allowed_env_keys"])):
                raise ValueError("MCP process differs from approved runtime")
            if not Path(server.command).is_absolute() or any(arg.split("=", 1)[0].lower() in forbidden_flags for arg in server.args):
                raise ValueError("MCP command authentication/relative executables are unsupported")
            for value in [*server.args, *server.env.values()]:
                if any(control and control in value for control in controls):
                    raise ValueError("Control credentials cannot enter MCP processes")
            if any(key.upper() in {"DATABASE_URL", "DATABASE_URI", "REDIS_URL", "DOCKER_HOST", "FLEET_NODE_TOKEN", "FLEET_ATTEMPT_TOKEN"} for key in server.env):
                raise ValueError("Control environment cannot enter MCP processes")
        else:
            raise ValueError("Unsupported MCP transport")


def validate_secret_bindings(private, spec, bundle):
    bindings = bundle["secret_bindings"]
    if not isinstance(bindings, dict):
        raise ValueError("Invalid operator secret bindings")
    model_targets, mcp_targets = set(), set()
    for ref in spec.secret_refs:
        binding = bindings.get(ref.reference_id)
        if not isinstance(binding, dict) or set(binding) != {"name", "kind", "target"} or binding["name"] != ref.name:
            raise ValueError("Launch secret reference is not operator-approved")
        if binding["kind"] == "model" and binding["target"] in {model.name for model in private.models}:
            model_targets.add(binding["target"])
        elif binding["kind"] == "mcp" and binding["target"] in bundle["mcp_servers"]:
            mcp_targets.add(binding["target"])
        else:
            raise ValueError("Unsupported or unknown launch secret target")
    for name, server in private.extensions.get_enabled_mcp_servers().items():
        authentication = bool(server.oauth or server.user_auth or server.headers_from_context or server.headers or server.env)
        if authentication and name not in mcp_targets:
            raise ValueError("MCP authentication requires its declared launch secret reference")
    return model_targets, mcp_targets


def execution_configuration(private):
    from deerflow.config.app_config import AppConfig
    from deerflow.models.credentials import AUTH_FIELDS

    payload = private.model_dump(mode="json")
    credentials = {}
    for model in payload["models"]:
        if model.get("default_headers") or model.get("default_query"):
            raise ValueError("Custom model authentication headers/query are unsupported in Agent bootstrap")
        credentials[(model["name"], model["use"])] = {key: model.pop(key) for key in tuple(model) if key in AUTH_FIELDS and model[key] is not None}
    # These host control-plane sections are never visible to graph tools. Real
    # RunContext resources are constructed separately from the private config.
    for key in ("auth", "database", "checkpointer", "stream_bridge", "channel_connections", "scheduler", "dedupe_storage"):
        payload.pop(key, None)
    payload["database"] = {"backend": "memory", "checkpoint_channel_mode": private.database.checkpoint_channel_mode, "checkpoint_delta": private.database.checkpoint_delta.model_dump()}
    payload["skills"]["path"] = "/opt/deerflow/skills"
    if payload["sandbox"].get("ownership") is not None:
        payload["sandbox"]["ownership"]["redis_url"] = None
    for plugin in payload["plugins"]:
        plugin["config"] = {}
    # MCP endpoints/auth/client setup live in the private scoped snapshot.
    payload["extensions"] = {"middlewares": private.extensions.middlewares, "skills": {name: state.model_dump() for name, state in private.extensions.skills.items()}}
    if private.acp_agents:
        raise ValueError("ACP subprocess capability requires an approved parent-container binding")
    if private.sandbox.use != "deerflow.sandbox.local:LocalSandboxProvider":
        raise ValueError("Agent tools must execute inside the parent container")

    # A second-pass detector catches accidentally retained authentication shapes
    # in arbitrary provider extras. No generic kwargs credential channel exists.
    def safe(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if ((key.lower() in {"token", "authorization"} and not isinstance(child, (dict, list))) or any(part in key.lower() for part in ("password", "credential", "secret", "api_key", "access_token", "github_token"))) and child:
                    raise ValueError("Unsupported raw credential in execution configuration")
                safe(child)
        elif isinstance(value, list):
            for child in value:
                safe(child)
        elif isinstance(value, str) and "://" in value:
            from urllib.parse import parse_qs, urlsplit

            url = urlsplit(value)
            if url.username is not None or url.password is not None or any(key.lower() in {"token", "authorization", "api_key", "key", "password", "secret", "access_token"} for key in parse_qs(url.query)):
                raise ValueError("Unsupported URL authentication in execution configuration")

    safe(payload)
    execution = AppConfig.model_validate(payload)

    def resolver(name, use):
        if (name, use) not in credentials:
            raise ValueError("Model provider is not operator-approved")
        return dict(credentials[(name, use)])

    return execution, resolver


def validate_model_bindings(private, spec, bindings):
    if not isinstance(bindings, dict) or not bindings:
        raise ValueError("Approved model bindings required")
    for name, binding in bindings.items():
        if not isinstance(binding, dict) or set(binding) != {"provider_use", "target_model", "version"} or not all(isinstance(value, str) and value for value in binding.values()):
            raise ValueError("Invalid approved model binding")
        model = next((item for item in private.models if item.name == name), None)
        if model is not None and any(set(model.model_dump().get(branch) or {}) & {"model", "use"} for branch in ("when_thinking_enabled", "when_thinking_disabled")):
            raise ValueError("Thinking branch cannot override approved model target")
        if model is None or (model.use, model.model) != (binding["provider_use"], binding["target_model"]):
            raise ValueError("Actual model configuration drifted from approved binding")
    if {model.name for model in private.models} != set(bindings):
        raise ValueError("Model is outside approved runtime bindings")
    if spec.model_name not in bindings or bindings[spec.model_name]["version"] != spec.model_version:
        raise ValueError("Launch model version is incompatible")


async def build_agent_environment(*, bootstrap, spec, grant):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.scheduler import ScheduledTaskService
    from deerflow.agents.lead_agent.agent import assemble_lead_agent
    from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
    from deerflow.extensions import load_extensions, reset_loaded_extensions, set_loaded_extensions
    from deerflow.extensions.gateway import start_services, stop_services
    from deerflow.extensions.notify import reset_extension_notify_loop, set_extension_notify_loop
    from deerflow.models.credentials import model_credential_scope
    from deerflow.persistence.agents.sql import SqlAgentStore
    from deerflow.persistence.managed_subagents.sql import SqlManagedSubagentStore
    from deerflow.persistence.mcp_tasks import McpTaskRepository
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.persistence.scheduled_task_runs import ScheduledTaskRunRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime import RunManager
    from deerflow.runtime.checkpoint_mode import freeze_checkpoint_channel_mode, freeze_checkpoint_snapshot_frequency
    from deerflow.runtime.checkpointer.async_provider import make_checkpointer
    from deerflow.runtime.events.store.db import DbRunEventStore
    from deerflow.runtime.runs.worker import RunContext
    from deerflow.runtime.store.async_provider import make_store
    from deerflow.runtime.stream_bridge import make_stream_bridge

    from .execution import decode_graph_input

    private = AppConfig.model_validate(bootstrap.operator_config)
    from .runtime import validate_remote_agent_shared_storage

    validate_remote_agent_shared_storage(private)
    if private.database.backend != "postgres" or not private.database.postgres_url or private.run_events.backend != "db":
        raise ValueError("Agent infrastructure requires shared Postgres and durable events")
    bindings = json.loads(Path("/opt/deerflow/model-bindings.json").read_bytes())
    validate_model_bindings(private, spec, bindings)
    validate_runtime_configuration(private)
    _, approved_bundle = runtime_bundle()
    authorized_models, _ = validate_secret_bindings(private, spec, approved_bundle)
    execution, private_resolver = execution_configuration(private)

    def resolver(name, use):
        values = private_resolver(name, use)
        if values and name not in authorized_models:
            raise ValueError("Model authentication requires its declared launch secret reference")
        return values

    compatibility = installed_compatibility()
    if (spec.runtime_digest, spec.skill_snapshot, spec.plugin_snapshot) != (compatibility.runtime_digest, compatibility.skill_snapshot, compatibility.plugin_snapshot):
        raise ValueError("Installed Agent runtime is incompatible")
    stack = AsyncExitStack()
    try:
        engine = create_async_engine(private.database.postgres_url, connect_args={"server_settings": {"search_path": private.database.postgres_schema}})
        stack.push_async_callback(engine.dispose)
        sf = async_sessionmaker(engine, expire_on_commit=False)
        checkpointer = await stack.enter_async_context(make_checkpointer(private, write_fence=FleetCheckpointFence(bootstrap.identity, spec)))
        store = await stack.enter_async_context(make_store(private))
        from deerflow.runtime.execution.mutation_context import remote_mutation_scope

        mutation_capability = FleetMutationCapability(bootstrap.identity, spec)
        repository = RunRepository(sf, mutation_capability=mutation_capability)
        manager = RunManager(store=repository, worker_id=bootstrap.identity.owner_worker_id)
        bridge = await stack.enter_async_context(make_stream_bridge(private))
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        if private.agent_storage.backend == "db":
            sync_engine = create_engine(private.database.app_sync_sqlalchemy_url, connect_args={"options": "-csearch_path=" + private.database.postgres_schema})
            stack.callback(sync_engine.dispose)
            sync_sf = sessionmaker(sync_engine, expire_on_commit=False)
            definitions = (SqlAgentStore(private.database.app_sync_sqlalchemy_url, session_factory=sync_sf), SqlManagedSubagentStore(private.database.app_sync_sqlalchemy_url, session_factory=sync_sf))
        else:
            from deerflow.persistence.agents import make_agent_store
            from deerflow.persistence.managed_subagents import make_managed_subagent_store

            definitions = (make_agent_store(private), make_managed_subagent_store(private))
        from deerflow.config.extensions_config import extensions_config_scope
        from deerflow.mcp.cache import mcp_tools_scope
        from deerflow.mcp.tools import get_mcp_tools

        private_tools = None
        from deerflow.persistence.agent_definition_context import agent_definition_store_scope

        @contextmanager
        def private_scope():
            with ExitStack() as scoped:
                from deerflow.runtime.execution.mutation_context import remote_mutation_scope

                scoped.enter_context(remote_mutation_scope(mutation_capability.context))
                scoped.enter_context(model_credential_scope(resolver))
                scoped.enter_context(extensions_config_scope(private.extensions))
                scoped.enter_context(agent_definition_store_scope(*definitions))
                if private_tools is not None:
                    scoped.enter_context(mcp_tools_scope(private_tools))
                yield

        set_app_config(execution)
        stack.callback(reset_app_config)
        from deerflow.mcp.session_pool import get_session_pool

        stack.push_async_callback(get_session_pool().close_all)
        with private_scope():
            private_tools = await get_mcp_tools()
            extensions, diagnostics = load_extensions(private.plugins)
        if any(item.level == "error" for item in diagnostics):
            raise ValueError("Approved runtime plugin failed to initialize")
        set_loaded_extensions(extensions)
        stack.callback(reset_loaded_extensions)
        import asyncio

        set_extension_notify_loop(asyncio.get_running_loop())
        stack.callback(reset_extension_notify_loop)
        attempted_services = []

        async def stop_plugins():
            with private_scope():
                await stop_services(extensions, service_entries=attempted_services)

        stack.push_async_callback(stop_plugins)
        with private_scope():
            service_diagnostics = await start_services(extensions, private, sf, attempted_services=attempted_services)
        if any(item.level == "error" for item in service_diagnostics):
            raise ValueError("Approved runtime plugin service failed to initialize")

        async def no_new_admission(**kwargs):
            raise RuntimeError("Agent runner cannot admit another scheduled run")

        scheduled = ScheduledTaskService(
            task_repo=ScheduledTaskRepository(sf, run_repository=repository),
            task_run_repo=ScheduledTaskRunRepository(sf, run_repository=repository),
            launch_run=no_new_admission,
            poll_interval_seconds=private.scheduler.poll_interval_seconds,
            lease_seconds=private.scheduler.lease_seconds,
            max_concurrent_runs=private.scheduler.max_concurrent_runs,
            queue_timeout_seconds=private.scheduler.queue_timeout_seconds,
        )
        context = RunContext(
            checkpointer=checkpointer,
            store=store,
            event_store=DbRunEventStore(sf, max_trace_content=private.run_events.max_trace_content, mutation_capability=mutation_capability),
            run_events_config=private.run_events,
            thread_store=ThreadMetaRepository(sf, mutation_capability=mutation_capability),
            mcp_task_repo=McpTaskRepository(sf),
            app_config=execution,
            extensions=extensions,
            checkpoint_channel_mode=freeze_checkpoint_channel_mode(private.database.checkpoint_channel_mode),
            checkpoint_snapshot_frequency=freeze_checkpoint_snapshot_frequency(private.database.checkpoint_delta.snapshot_frequency),
            on_run_completed=scheduled.handle_run_completion,
        )

        async def close():
            try:
                await stack.aclose()
            finally:
                reset_app_config()

        return AgentEnvironment(
            identity=bootstrap.identity,
            mutation_scope=lambda: remote_mutation_scope(mutation_capability.context),
            context=context,
            manager=manager,
            bridge=bridge,
            agent_factory=lambda config: assemble_lead_agent(config, app_config=execution, expected_model_name=spec.model_name),
            compatibility=compatibility,
            credential_resolver=resolver,
            decode_input=lambda value: decode_graph_input(thaw(value)),
            close=close,
            private_extensions_config=private.extensions,
            definition_stores=definitions,
            private_mcp_tools=private_tools,
        )
    except BaseException:
        await stack.aclose()
        raise


# The same installed factory supplies preflight and private runtime construction.
# Optional Fleet discovers this attribute through metadata, never a host import.
build_agent_environment.worker_compatibility = installed_compatibility
