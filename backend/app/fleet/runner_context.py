"""Trusted host environment bridge. Private connections never enter tool config."""

import json
from contextlib import AsyncExitStack, ExitStack, contextmanager
from hashlib import sha256
from importlib import metadata
from pathlib import Path

from deerflow_ecs_fleet.launch_spec import Snapshot, WorkerCompatibility, thaw
from deerflow_ecs_fleet.worker.agent_cleanup import CleanupBudget, PendingAgentCleanup
from deerflow_ecs_fleet.worker.agent_environment import AgentEnvironment

from app.fleet.mutation import FleetCheckpointFence, FleetMutationCapability
from deerflow.runtime.execution.mutation_context import OwnershipRejected

# A failed bounded close keeps the complete original host resource owner alive.
# Neither this registry nor its retry callbacks enter graph configuration.
_pending_agent_teardowns = {}


class AgentCleanupPending(OwnershipRejected, PendingAgentCleanup):
    def __init__(self, teardown):
        super().__init__("Owned Agent cleanup is pending; original host resources retained")
        self._teardown = teardown

    @property
    def remaining_seconds(self):
        return self._teardown.budget.remaining()

    def cleanup_scope(self):
        return self._teardown.private_scope()

    def _check_original_scope(self):
        from deerflow.runtime.execution.mutation_context import current_remote_mutation_context

        if current_remote_mutation_context() != self._teardown.context:
            raise OwnershipRejected("Host cleanup retry requires its original private scope")

    async def wait_for_cleanup(self):
        from deerflow.extensions.notify import wait_extension_dispatch_cleanup

        self._check_original_scope()
        await self._teardown.settle_graph_stream()
        await self._teardown.phase("settlement", wait_extension_dispatch_cleanup)

    async def retry_cleanup(self):
        self._check_original_scope()
        await self._teardown.close()


class _AgentResourceTeardown:
    def __init__(self, stack, private_scope, context):
        self.stack, self.private_scope, self.context = stack, private_scope, context
        self.budget = CleanupBudget()
        self.stop_plugins = None
        self.settle_memory = None
        self.memory_quiescent = False
        self.plugins_stopped = False
        self.failure = None
        self.closed = False
        self._phases = {}

    def retain_pending(self, error):
        if self.failure is None:
            self.failure = error
        _pending_agent_teardowns[self.context] = self
        raise AgentCleanupPending(self) from error

    async def phase(self, name, operation):
        import asyncio

        task = self._phases.get(name)
        if task is None:
            if self.budget.remaining() <= 0:
                self.retain_pending(OwnershipRejected("Agent total cleanup deadline exceeded"))
            task = asyncio.create_task(operation())
            self._phases[name] = task
        try:
            completed, _ = await asyncio.wait({task}, timeout=self.budget.remaining())
        except asyncio.CancelledError as exc:
            # Retain the real phase Task; cancellation never grants unwind.
            self.retain_pending(exc)
        if not completed:
            self.retain_pending(OwnershipRejected("Agent total cleanup deadline exceeded"))
        self._phases.pop(name)
        return task.result()

    async def settle_graph_stream(self):
        if "graph-stream" not in self._phases:
            return
        try:
            await self.phase("graph-stream", None)
        except PendingAgentCleanup:
            raise
        except BaseException as error:
            # The real Task is settled. Retain its failure until the same stack
            # has safely unwound; bootstrap must not escape at the wait phase.
            if self.failure is None:
                self.failure = error

    async def quiesce_memory_and_observers(self):
        from deerflow.extensions.notify import drain_extension_dispatches, extension_dispatch_revision, extension_dispatches_pending

        self.memory_quiescent = False
        while True:
            revision = extension_dispatch_revision()
            await self.settle_memory()
            try:
                await drain_extension_dispatches(deadline=self.budget.deadline)
            except BaseException as error:
                if extension_dispatches_pending():
                    self.retain_pending(error)
                if self.failure is None:
                    self.failure = error
            # A callback that completed during the drain may have queued native
            # memory even though no observer is currently pending.
            if revision == extension_dispatch_revision():
                self.memory_quiescent = True
                return

    async def close(self):
        from deerflow.extensions.notify import drain_extension_dispatches, extension_dispatches_pending, release_extension_dispatch_failures

        if self.closed:
            return
        self.budget.start()
        with self.private_scope():
            await self.settle_graph_stream()
            if self.settle_memory is not None:
                await self.phase("quiescence-before-services", self.quiesce_memory_and_observers)
            else:
                try:
                    await self.phase("drain", lambda: drain_extension_dispatches(deadline=self.budget.deadline))
                except PendingAgentCleanup:
                    raise
                except BaseException as exc:
                    if extension_dispatches_pending():
                        self.retain_pending(exc)
                    if self.failure is None:
                        self.failure = exc
                if extension_dispatches_pending():
                    self.retain_pending(OwnershipRejected("Owned extension writer has not settled"))
            if self.stop_plugins is not None and not self.plugins_stopped:
                try:
                    await self.phase("plugins", self.stop_plugins)
                except PendingAgentCleanup:
                    raise
                except BaseException as exc:
                    if extension_dispatches_pending():
                        self.retain_pending(exc)
                    if self.failure is None:
                        self.failure = exc
                self.plugins_stopped = True
            if self.settle_memory is not None:
                await self.phase("quiescence-before-resources", self.quiesce_memory_and_observers)
            try:
                await self.phase("resources", self.stack.aclose)
            except PendingAgentCleanup:
                raise
            except BaseException as exc:
                if self.failure is None:
                    self.failure = exc
            self.closed = True
            release_extension_dispatch_failures()
            _pending_agent_teardowns.pop(self.context, None)
            if self.failure is not None:
                raise self.failure


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
    # Backend construction uses the private snapshot. Only the prompt's known
    # read-failure enum belongs in model/tool-visible configuration.
    memory_policy = private.memory.backend_config.get("failure_policy", {})
    read_policy = memory_policy.get("read") if isinstance(memory_policy, dict) else None
    payload["memory"]["backend_config"] = {"failure_policy": {"read": read_policy}} if read_policy in ("fail_closed", "fail_open") else {}

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

    from .execution import decode_graph_input

    private = AppConfig.model_validate(bootstrap.operator_config)
    from .runtime import validate_remote_agent_shared_storage

    if private.agent_storage.backend != "db":
        raise ValueError("Remote Agent definitions require a fenced database backend")
    validate_remote_agent_shared_storage(private)
    from deerflow.agents.memory.manager import make_remote_memory_manager, memory_manager_scope, preflight_remote_memory

    preflight_remote_memory(private.memory)
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
    from deerflow.config.app_config import pop_current_app_config, push_current_app_config
    from deerflow.runtime.execution.mutation_context import remote_mutation_scope

    mutation_capability = FleetMutationCapability(bootstrap.identity, spec)

    @contextmanager
    def bootstrap_cleanup_scope():
        with remote_mutation_scope(mutation_capability.context), model_credential_scope(resolver):
            push_current_app_config(execution)
            try:
                yield
            finally:
                pop_current_app_config()

    stack = AsyncExitStack()
    teardown = _AgentResourceTeardown(stack, bootstrap_cleanup_scope, mutation_capability.context)
    try:
        engine = create_async_engine(private.database.postgres_url, connect_args={"server_settings": {"search_path": private.database.postgres_schema}})
        stack.push_async_callback(engine.dispose)
        sf = async_sessionmaker(engine, expire_on_commit=False)
        checkpointer = await stack.enter_async_context(make_checkpointer(private, write_fence=FleetCheckpointFence(bootstrap.identity, spec)))
        from deerflow.runtime.execution.mutation_context import remote_mutation_scope

        store = await stack.enter_async_context(make_store(private, mutation_capability=mutation_capability))
        repository = RunRepository(sf, mutation_capability=mutation_capability)
        manager = RunManager(store=repository, worker_id=bootstrap.identity.owner_worker_id)
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        if private.agent_storage.backend == "db":
            sync_engine = create_engine(private.database.app_sync_sqlalchemy_url, connect_args={"options": "-csearch_path=" + private.database.postgres_schema})
            stack.callback(sync_engine.dispose)
            sync_sf = sessionmaker(sync_engine, expire_on_commit=False)
            definitions = (
                SqlAgentStore(private.database.app_sync_sqlalchemy_url, session_factory=sync_sf, mutation_capability=mutation_capability),
                SqlManagedSubagentStore(private.database.app_sync_sqlalchemy_url, session_factory=sync_sf, mutation_capability=mutation_capability),
            )
        else:
            from deerflow.persistence.agents import make_agent_store
            from deerflow.persistence.managed_subagents import make_managed_subagent_store

            definitions = (make_agent_store(private), make_managed_subagent_store(private))
        from app.mcp_tasks import McpTaskService
        from deerflow.config.extensions_config import extensions_config_scope
        from deerflow.mcp.cache import mcp_tools_scope
        from deerflow.mcp.task_tool_caller import McpTaskToolCaller
        from deerflow.mcp.tasks import ORDINARY_MCP_TASK_DRIVER, McpTaskDriverRegistry, OrdinaryMcpTaskDriver
        from deerflow.mcp.tasks.runtime import mcp_task_submitter_scope, validate_mcp_task_runtime_configuration
        from deerflow.mcp.tools import get_mcp_tools

        mcp_repository = McpTaskRepository(sf, mutation_capability=mutation_capability)
        validate_mcp_task_runtime_configuration(mcp_tasks_config=private.mcp_tasks, extensions_config=private.extensions, repository_available=True)
        drivers = McpTaskDriverRegistry()
        drivers.register(ORDINARY_MCP_TASK_DRIVER, OrdinaryMcpTaskDriver(McpTaskToolCaller(private.extensions)))
        private_submitter = McpTaskService(
            repository=mcp_repository, drivers=drivers, poll_interval_seconds=private.mcp_tasks.poll_interval_seconds, lease_seconds=private.mcp_tasks.lease_seconds, max_concurrent_polls=private.mcp_tasks.max_concurrent_polls
        )
        private_tools = None
        private_memory = None
        from deerflow.persistence.agent_definition_context import agent_definition_store_scope

        @contextmanager
        def private_scope():
            with ExitStack() as scoped:
                from deerflow.runtime.execution.mutation_context import remote_mutation_scope

                scoped.enter_context(remote_mutation_scope(mutation_capability.context))
                scoped.enter_context(model_credential_scope(resolver))
                scoped.enter_context(extensions_config_scope(private.extensions))
                scoped.enter_context(mcp_task_submitter_scope(private_submitter, private.extensions))
                scoped.enter_context(agent_definition_store_scope(*definitions))
                if private_memory is not None:
                    scoped.enter_context(memory_manager_scope(private_memory))
                if private_tools is not None:
                    scoped.enter_context(mcp_tools_scope(private_tools))
                yield

        # Same fixed owner, stack and budget; complete private resources now exist.
        teardown.private_scope = private_scope
        set_app_config(execution)
        stack.callback(reset_app_config)
        from deerflow.mcp.session_pool import get_session_pool

        stack.push_async_callback(get_session_pool().close_all)
        with private_scope():
            private_tools = await get_mcp_tools()
            extensions, diagnostics = load_extensions(private.plugins)
        if any(item.level == "error" for item in diagnostics):
            raise ValueError("Approved runtime plugin failed to initialize")
        from deerflow.extensions.gateway import bind_remote_extensions

        extensions = bind_remote_extensions(extensions, mutation_capability)
        with private_scope():
            private_memory = make_remote_memory_manager(private.memory, mutation_capability=mutation_capability, host_hooks={"session_factory": sync_sf, "async_session_factory": sf})

        async def settle_memory():
            import asyncio

            with private_scope():
                while True:
                    remaining = teardown.budget.remaining()
                    if remaining <= 0:
                        teardown.retain_pending(OwnershipRejected("Agent total memory cleanup deadline exceeded"))
                    try:
                        complete = await asyncio.to_thread(private_memory.shutdown_flush, min(30, remaining))
                    except BaseException as error:
                        if teardown.failure is None:
                            teardown.failure = error
                    else:
                        if complete is True:
                            return
                        if teardown.failure is None:
                            teardown.failure = OwnershipRejected("Owned memory worker drain is incomplete")
                    # Some backends return False/raise immediately. Preserve the
                    # same phase and deadline without spinning the owner loop.
                    await asyncio.sleep(min(0.05, teardown.budget.remaining()))

        # Attach immediately: construction/start failure can already enqueue.
        teardown.settle_memory = settle_memory

        async def close_memory():
            import asyncio

            with private_scope():
                if not teardown.memory_quiescent:
                    raise OwnershipRejected("Memory resources require positive joint quiescence")
                await asyncio.to_thread(private_memory.close)

        stack.push_async_callback(close_memory)
        set_loaded_extensions(extensions)
        stack.callback(reset_loaded_extensions)
        import asyncio

        set_extension_notify_loop(asyncio.get_running_loop())
        stack.callback(reset_extension_notify_loop)
        attempted_services = []
        services_stopped = False

        async def stop_plugins():
            from deerflow.extensions.notify import extension_dispatches_pending

            nonlocal services_stopped

            async def drain():
                await teardown.quiesce_memory_and_observers()

            async def stop_once():
                nonlocal services_stopped
                if not services_stopped:
                    try:
                        await stop_services(extensions, service_entries=attempted_services, deadline=teardown.budget.deadline)
                    except OwnershipRejected:
                        # Typed stop loss reports after all services were attempted.
                        services_stopped = True
                        raise
                    services_stopped = True

            with private_scope():
                failure = None
                for cleanup in (drain, stop_once, drain):
                    try:
                        await cleanup()
                    except BaseException as exc:
                        if extension_dispatches_pending():
                            raise
                        if failure is None:
                            failure = exc
                if failure is not None:
                    raise failure

        # Plugin settlement must precede ExitStack unwind: a failing callback
        # cannot stop AsyncExitStack from closing its remaining resources.
        teardown.stop_plugins = stop_plugins
        with private_scope():
            service_diagnostics = await start_services(extensions, private, sf, attempted_services=attempted_services, mutation_capability=mutation_capability, sync_session_factory=sync_sf)
        if any(item.level == "error" for item in service_diagnostics):
            raise ValueError("Approved runtime plugin service failed to initialize")

        async def no_new_admission(**kwargs):
            raise RuntimeError("Agent runner cannot admit another scheduled run")

        scheduled = ScheduledTaskService(
            task_repo=ScheduledTaskRepository(sf, run_repository=repository, mutation_capability=mutation_capability),
            task_run_repo=ScheduledTaskRunRepository(sf, run_repository=repository, mutation_capability=mutation_capability),
            launch_run=no_new_admission,
            poll_interval_seconds=private.scheduler.poll_interval_seconds,
            lease_seconds=private.scheduler.lease_seconds,
            max_concurrent_runs=private.scheduler.max_concurrent_runs,
            queue_timeout_seconds=private.scheduler.queue_timeout_seconds,
        )

        async def drain_active_memory():
            with private_scope():
                await drain_remote_mutations(private_memory)

        async def settle_owned_stream(stream):
            from deerflow.runtime.execution.mutation_context import ExecutionCleanupPending, current_remote_mutation_context

            if current_remote_mutation_context() != teardown.context:
                raise OwnershipRejected("Graph cleanup requires its original private scope")
            with private_scope():
                try:
                    await teardown.phase("graph-stream", stream.aclose)
                except AgentCleanupPending as pending:
                    raise ExecutionCleanupPending() from pending

        from deerflow_ecs_fleet.persistence.outbox import EventOutbox

        from app.fleet.events import FleetEventParticipant, FleetProducerBridge, FleetStreamSeals, RemoteStreamIdentity

        stream_identity = RemoteStreamIdentity.from_context(mutation_capability.context)
        participant = FleetEventParticipant(identity=stream_identity, spec=spec, capability=mutation_capability, outbox=EventOutbox())
        event_store = DbRunEventStore(sf, max_trace_content=private.run_events.max_trace_content, mutation_capability=mutation_capability, transaction_participant=participant)
        bridge = FleetProducerBridge(event_store=event_store, identity=stream_identity, spec=spec, capability=mutation_capability, seals=FleetStreamSeals(sf), manager=manager)

        context = RunContext(
            checkpointer=checkpointer,
            store=store,
            event_store=event_store,
            run_events_config=private.run_events,
            thread_store=ThreadMetaRepository(sf, mutation_capability=mutation_capability),
            mcp_task_repo=mcp_repository,
            app_config=execution,
            extensions=extensions,
            checkpoint_channel_mode=freeze_checkpoint_channel_mode(private.database.checkpoint_channel_mode),
            checkpoint_snapshot_frequency=freeze_checkpoint_snapshot_frequency(private.database.checkpoint_delta.snapshot_frequency),
            on_run_completed=scheduled.handle_run_completion,
            before_terminal_mutations=drain_active_memory,
            settle_stream=settle_owned_stream,
        )

        async def close():
            await teardown.close()

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
            private_memory_manager=private_memory,
            private_mcp_task_submitter=private_submitter,
        )
    except BaseException as original_error:
        try:
            await teardown.close()
        except AgentCleanupPending as pending:
            pending._original_error = original_error
            raise
        except BaseException as cleanup_error:
            raise original_error from cleanup_error
        raise


# The same installed factory supplies preflight and private runtime construction.
# Optional Fleet discovers this attribute through metadata, never a host import.
build_agent_environment.worker_compatibility = installed_compatibility


async def drain_remote_mutations(memory, *, timeout=30):
    """Finish this attempt's queued active writes before durable completion."""
    import asyncio

    from deerflow.extensions.notify import drain_extension_dispatches

    try:
        async with asyncio.timeout(timeout):
            if not await asyncio.to_thread(memory.shutdown_flush, timeout):
                raise OwnershipRejected("Remote memory drain exceeded its completion budget")
            # Extraction itself can enqueue model observations. Drain callbacks
            # including nested dispatches while active writes remain authorized.
            await drain_extension_dispatches()
    except TimeoutError as exc:
        raise OwnershipRejected("Remote mutation drain exceeded its completion budget") from exc
