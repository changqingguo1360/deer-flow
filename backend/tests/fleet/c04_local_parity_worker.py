"""Actual Native run in a fresh process, outside pytest's executor substitutes."""

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace


async def main(payload):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.gateway.routers.thread_runs import RunCreateRequest
    from deerflow.config.app_config import reset_app_config
    from deerflow.runtime.user_context import reset_current_user, set_current_user
    from fleet.c04_integration_fixture import local_parity_run, semantic_messages

    private = payload["private"]
    engine = create_async_engine(payload["host_url"], connect_args={"server_settings": {"search_path": payload["schema"]}})
    db = SimpleNamespace(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False), host_url=payload["host_url"], schema=payload["schema"])
    user = SimpleNamespace(id=payload["user_id"], system_role="admin")
    token = set_current_user(user)
    try:
        local, remote, artifacts = await local_parity_run(db, private, RunCreateRequest.model_validate(payload["body"]), user, Path(payload["directory"]))
        result = {
            "local": semantic_messages(local),
            "remote": semantic_messages(remote),
            "artifacts": {name: value.hex() for name, value in artifacts.items()},
            "todos_equal": local.checkpoint["channel_values"].get("todos") == remote.checkpoint["channel_values"].get("todos"),
            "different_checkpoint_ids": local.config["configurable"]["checkpoint_id"] != remote.config["configurable"]["checkpoint_id"],
            "has_parent_refs": local.parent_config is not None and remote.parent_config is not None,
            "pid": __import__("os").getpid(),
        }
        print(json.dumps(result), flush=True)
    finally:
        reset_current_user(token)
        reset_app_config()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main(json.loads(sys.stdin.buffer.readline())))
