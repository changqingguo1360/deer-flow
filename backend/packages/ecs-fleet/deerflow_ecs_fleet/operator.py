"""Trusted host operator commands; database secrets and issued tokens stay in files."""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .config import NAME_PATTERN, FleetConfig
from .service import FleetService
from .worker.__main__ import read_file


class OperatorSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    database_url: str = Field(repr=False)
    database_schema: str | None = Field(default=None, alias="schema", pattern=NAME_PATTERN)
    fleet: FleetConfig = Field(default_factory=FleetConfig)


async def operate(args, settings):
    if not settings.fleet.enabled or not settings.database_url.startswith("postgresql+asyncpg://"):
        raise ValueError("Explicit enabled Fleet and host Postgres required")
    kwargs = {"connect_args": {"server_settings": {"search_path": settings.database_schema}}} if settings.database_schema else {}
    engine = create_async_engine(settings.database_url, **kwargs)
    service = FleetService(settings.fleet)
    try:
        await service.start(SimpleNamespace(session_factory=async_sessionmaker(engine, expire_on_commit=False)))
        if args.action == "register":
            await service.nodes.register(node_id=args.node_id, name=args.name, cpu_millis=args.cpu_millis, memory_mib=args.memory_mib)
        elif args.action == "issue":
            if not 1 <= args.lifetime_seconds <= 31_536_000 or not args.credential_file.is_absolute():
                raise ValueError("Invalid credential destination or lifetime")
            # Reserve destination before issuing: never overwrite a live secret.
            fd = os.open(args.credential_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            issued = None
            try:
                os.fchmod(fd, 0o600)
                issued = await service.credentials.issue(args.node_id, lifetime_seconds=args.lifetime_seconds)
                with os.fdopen(fd, "w") as stream:
                    fd = None
                    stream.write(issued.token + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                parent = os.open(args.credential_file.parent, os.O_RDONLY)
                try:
                    os.fsync(parent)
                finally:
                    os.close(parent)
            except BaseException:
                if fd is not None:
                    os.close(fd)
                if issued is not None:
                    await service.credentials.revoke(issued.credential_id)
                args.credential_file.unlink(missing_ok=True)
                raise
            return {"credential_id": issued.credential_id, "node_id": issued.node_id}
        elif args.action == "revoke":
            await service.credentials.revoke(args.credential_id)
        elif args.action in {"drain", "disable", "enable"}:
            await service.nodes.set_admin_state(args.node_id, {"drain": "draining", "disable": "disabled", "enable": "enabled"}[args.action])
        if args.action != "revoke":
            return await service.nodes.status(args.node_id)
        return {"revoked": True}
    finally:
        await service.stop()
        await engine.dispose()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Trusted Fleet operator on an existing bootstrapped host Postgres database")
    parser.add_argument("--settings", required=True, type=Path, help="Private JSON settings containing database_url and Fleet config")
    actions = parser.add_subparsers(dest="action", required=True)
    for name in ("register", "issue", "drain", "disable", "enable", "status", "revoke"):
        command = actions.add_parser(name)
        if name == "revoke":
            command.add_argument("--credential-id", required=True)
        else:
            command.add_argument("--node-id", required=True)
        if name == "register":
            command.add_argument("--name", required=True)
            command.add_argument("--cpu-millis", required=True, type=int)
            command.add_argument("--memory-mib", required=True, type=int)
        if name == "issue":
            command.add_argument("--credential-file", required=True, type=Path)
            command.add_argument("--lifetime-seconds", required=True, type=int)
    args = parser.parse_args(argv)
    try:
        settings = OperatorSettings.model_validate(json.loads(read_file(args.settings, private=True)))
        print(json.dumps(asyncio.run(operate(args, settings))))
    except Exception:
        print("Fleet operator action failed; no secrets are printed. Check private configuration and retained execution state.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
