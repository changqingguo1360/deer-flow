"""Installed trusted extension tests actual private resource lifecycle bindings."""

import json
import os
from pathlib import Path


def install(registry, config):
    from deerflow.config.app_config import get_app_config
    from deerflow.models.factory import create_chat_model

    model = create_chat_model("model-1", app_config=get_app_config(), attach_tracing=False)
    if model.api_key is None:
        raise ValueError("Plugin load has no approved provider binding")
    registry.service(Service())


class Service:
    async def check(self, phase):
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
        path = Path(os.environ["DEER_FLOW_HOME"]) / "plugin-lifecycle.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        phases = json.loads(path.read_text()) if path.exists() else []
        phases.append(phase)
        path.write_text(json.dumps(phases))

    async def start(self, deps):
        await self.check("start")

    async def stop(self):
        await self.check("stop")
