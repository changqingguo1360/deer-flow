"""Fleet schema lifecycle follows host persistence startup and shares no models."""

import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import text

from .config import FleetConfig

MIGRATION_LOCK = 73462101


def upgrade_connection(connection):
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
    config.attributes["connection"] = connection
    command.upgrade(config, "head")


class FleetService:
    fleet_protocol_version = 1

    def __init__(self, config: FleetConfig):
        self.config = config
        self.ready = False
        self.session_factory = None
        self.credentials = None
        self.nodes = None
        self.scheduler = None
        self.attempts = None
        self.jobs = None
        self.reconciler = None
        self.workspace = None
        self.manifests = None
        self.inputs = None
        self.recovery = None
        self.management = None
        self._continuation_tasks = []
        self._continuation_stop = asyncio.Event()

    async def start(self, deps) -> None:
        self._continuation_stop = asyncio.Event()
        self.ready = False
        if deps.session_factory is None:
            raise ValueError("Fleet requires a Postgres session factory")
        engine = deps.session_factory.kw.get("bind")
        if engine is None:
            raise ValueError("Fleet requires a bound Postgres session factory")
        self.config.validate_host(database_backend=engine.dialect.name)
        if self.config.nas_root is not None and self.config.nas_identity is not None:
            from .workspace import NASWorkspace

            self.workspace = NASWorkspace(self.config.nas_root, identity=self.config.nas_identity)
            await asyncio.to_thread(self.workspace.validate_root)
        async with engine.begin() as connection:
            await connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": MIGRATION_LOCK})
            await connection.run_sync(upgrade_connection)
        from .node_credentials import NodeCredentials
        from .nodes import NodeRegistry
        from .persistence.attempts import JobAttempts
        from .scheduler import FleetScheduler

        self.session_factory = deps.session_factory
        self.credentials = NodeCredentials(deps.session_factory)
        self.nodes = NodeRegistry(
            deps.session_factory,
            configured_profiles=self.config.profiles,
            agent_profiles=(name for name, profile in self.config.profiles.items() if profile.kind == "agent"),
            default_profiles=(name for name, profile in self.config.profiles.items() if profile.kind == "job"),
        )
        self.scheduler = FleetScheduler(deps.session_factory, self.config)
        self.attempts = JobAttempts(deps.session_factory, self.config)
        from .recovery import FleetRecovery

        self.recovery = FleetRecovery(deps.session_factory, attempts=self.attempts)
        if self.workspace is not None:
            from .persistence.inputs import InputManifests
            from .persistence.manifests import FleetManifests

            self.inputs = InputManifests(deps.session_factory, workspace=self.workspace, config=self.config)
            self.manifests = FleetManifests(deps.session_factory, attempts=self.attempts, workspace=self.workspace)
        from .management import FleetManagement

        self.management = FleetManagement(self)
        self.ready = True

    def bind_tracking(self, reader):
        if self.jobs is not None:
            raise RuntimeError("Fleet tracking already bound")
        from .job_service import FleetJobService
        from .mcp_driver import FleetTaskDriver
        from .reconcile import JobReconciler

        self.jobs = FleetJobService(self.session_factory, self.config, tracking_reader=reader)
        self.reconciler = JobReconciler(self.jobs, self.attempts)
        self.reconciler.start()
        return FleetTaskDriver(self.jobs)

    def bind_continuations(self, scan):
        """Host injection owns admission; service owns restartable scan lifecycle."""

        async def run():
            import logging

            while not self._continuation_stop.is_set():
                try:
                    await scan()
                except Exception:
                    logging.getLogger(__name__).exception("Fleet continuation scan failed")
                try:
                    await asyncio.wait_for(self._continuation_stop.wait(), timeout=1)
                except TimeoutError:
                    pass

        self._continuation_tasks.append(asyncio.create_task(run(), name="fleet-continuations"))

    async def stop(self) -> None:
        self._continuation_stop.set()
        await asyncio.gather(*self._continuation_tasks)
        self._continuation_tasks.clear()
        self.ready = False
        if self.reconciler is not None:
            await self.reconciler.stop()
            self.reconciler = None
        self.jobs = None
        self.session_factory = None
        self.credentials = None
        self.nodes = None
        self.scheduler = None
        self.attempts = None
        self.workspace = None
        self.manifests = None
        self.inputs = None
        self.recovery = None
        self.management = None
