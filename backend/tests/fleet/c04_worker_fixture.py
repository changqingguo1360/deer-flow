"""Deterministic provider used with the real lead and delegated agent graphs."""

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from pydantic import SecretStr


class ScriptedModel(BaseChatModel):
    model: str = "parent"
    api_key: SecretStr | None = None
    c07_gate: bool = False

    @property
    def _llm_type(self):
        return "c04-scripted"

    def bind_tools(self, tools, **kwargs):
        return self

    def reply(self, messages):
        if self.api_key is None:
            raise ValueError("Approved provider credential required")
        from deerflow.config.extensions_config import get_scoped_extensions_config
        from deerflow.persistence.agent_definition_context import get_scoped_definition_stores

        if get_scoped_extensions_config() is None or get_scoped_definition_stores() is None:
            raise ValueError("Trusted infrastructure scopes were not propagated")
        from deerflow.config.app_config import get_app_config

        if self.api_key.get_secret_value() in get_app_config().model_dump_json():
            raise ValueError("Provider credential leaked to execution view")
        tools = [m for m in messages if isinstance(m, ToolMessage)]
        index = len(tools)
        from deerflow.skills import get_or_new_skill_storage

        skills = {skill.name: skill.enabled for skill in get_or_new_skill_storage().load_skills(enabled_only=False)}
        if skills.get("c04-enabled") is not True or skills.get("c04-disabled") is not False:
            raise ValueError("Actual bundled skill state was not preserved")
        if self.c07_gate:
            from fleet.c07_integration_fixture import BarrierModel

            return BarrierModel().answer(messages)
        names = {message.name for message in tools}
        calls = []
        config = get_app_config()
        memory_enabled = config.memory.enabled and config.memory.manager_class == "deerflow_c04_fixture.memory:PostgresMemory"
        if memory_enabled and config.memory.mode == "tool" and not any(message.tool_call_id == "c04-" + self.model + "-memory" for message in tools):
            calls = [{"id": "c04-" + self.model + "-memory", "type": "tool_call", "name": "memory_add", "args": {"content": "c04-" + self.model + "-memory"}}]
        elif "bash" not in names:
            output = "child.txt" if self.model == "child" else "parent.txt"
            import re

            port_match = re.search(r"c04-control-port=(\d+)", str(messages))
            port = port_match.group(1) if port_match else "0"
            tool_entry = "-m fleet.c04_tool_probe" if __import__("sys").platform == "linux" else "/mnt/c04-runtime/c04_tool_probe.py"
            command = (
                "cat /mnt/user-data/workspace/source.txt /mnt/user-data/uploads/source.txt > /mnt/user-data/outputs/" + output + "; python " + tool_entry + " --port " + port + " --output /mnt/user-data/outputs/" + self.model + "-probe.json"
            )
            calls = [{"id": "c04-" + self.model + "-bash", "type": "tool_call", "name": "bash", "args": {"description": "Write deterministic artifact", "command": command}}]
        elif "c04_echo" not in names and self.model != "child":
            calls = [{"id": "c04-parent-mcp", "type": "tool_call", "name": "c04_echo", "args": {"value": "c04-mcp-result"}}]
        elif "c04_submit_job" not in names and self.model != "child":
            calls = [{"id": "c04-parent-durable-mcp", "type": "tool_call", "name": "c04_submit_job", "args": {"value": "c04-job-input"}}]
        elif "task" not in names and self.model != "child":
            calls = [
                {
                    "id": "c04-parent-task",
                    "type": "tool_call",
                    "name": "task",
                    "args": {
                        "description": "Delegated artifact",
                        "prompt": "child-work: create the child artifact c04-control-port=" + __import__("re").search(r"c04-control-port=(\d+)", str(messages)).group(1),
                        "subagent_type": "general-purpose",
                    },
                }
            ]
        elif "present_files" not in names and self.model != "child":
            calls = [{"id": "c04-parent-present", "type": "tool_call", "name": "present_files", "args": {"filepaths": ["/mnt/user-data/outputs/parent.txt", "/mnt/user-data/outputs/child.txt"]}}]
        return AIMessage(
            id="c04-" + self.model + "-" + str(index),
            content="" if calls else "c04-result [artifact](/mnt/user-data/outputs/" + ("child.txt" if self.model == "child" else "parent.txt") + ")",
            tool_calls=calls,
            usage_metadata={"input_tokens": 11, "output_tokens": 7, "total_tokens": 18},
        )

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        return ChatResult(generations=[ChatGeneration(message=self.reply(messages))])

    def _stream(self, messages, stop=None, run_manager=None, **kwargs):
        message = self.reply(messages)
        yield ChatGenerationChunk(message=AIMessageChunk(**message.model_dump(exclude={"type"})))
