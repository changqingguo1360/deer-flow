"""Installed trusted PostgreSQL extension exercises real lifecycle resources."""


def install(registry, config):
    from deerflow.config.app_config import get_app_config
    from deerflow.models.factory import create_chat_model

    model = create_chat_model("model-1", app_config=get_app_config(), attach_tracing=False)
    if model.api_key is None:
        raise ValueError("Plugin load has no approved provider binding")
    service = Service()
    registry.service(service)
    registry.task_lifecycle(service)


class Service:
    remote_state_mode = "transactional"

    async def check(self):
        from deerflow.config.app_config import get_app_config
        from deerflow.mcp.cache import get_cached_mcp_tools
        from deerflow.models.factory import create_chat_model
        from deerflow.persistence.agents import get_agent_store
        from deerflow.persistence.managed_subagents import get_managed_subagent_store

        model = create_chat_model("model-1", app_config=get_app_config(), attach_tracing=False)
        if model.api_key is None or model.api_key.get_secret_value() in get_app_config().model_dump_json():
            raise ValueError("Plugin lifecycle lost private provider binding")
        get_agent_store().list(user_id="user-c04")
        get_managed_subagent_store().list()
        tool = next(item for item in get_cached_mcp_tools() if item.name == "c04_echo")
        if "plugin-private-resource" not in str(await tool.ainvoke({"value": "plugin-private-resource"})):
            raise ValueError("Plugin lifecycle lost private MCP binding")

    async def write(self, value):
        from sqlalchemy import text

        async with self._deps.mutation_transactions.async_transaction() as session:
            await session.execute(text("INSERT INTO c06_extension(value) VALUES (:v)"), {"v": value})

    async def start(self, deps):
        self._deps = deps
        self._lead = None
        await self.check()
        await self.write("start")

    async def stop(self):
        await self.check()
        if self._lead is not None:
            # The only durable stop operation is a host-owned original-task
            # receipt; no terminal SQL session is available to this plugin.
            await self._deps.terminal_operations.record_task_stop(task_id=self._lead.task_id, outcome=self._outcome)

    async def on_task_start(self, app_store, task_store, info):
        if info.kind == "lead":
            self._lead = info
        await self.write(info.kind + "-start")

    async def on_task_stop(self, app_store, task_store, info, outcome):
        if info.kind == "lead":
            self._outcome = outcome.value
            await self._deps.terminal_operations.record_task_stop(task_id=info.task_id, outcome=outcome.value)
        else:
            await self.write("subagent-stop")
