"""Fleet schema lifecycle follows host persistence startup and shares no models."""

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
        self.jobs = None
        self.reconciler = None

    async def start(self, deps) -> None:
        self.ready = False
        if deps.session_factory is None:
            raise ValueError("Fleet requires a Postgres session factory")
        engine = deps.session_factory.kw.get("bind")
        if engine is None:
            raise ValueError("Fleet requires a bound Postgres session factory")
        self.config.validate_host(database_backend=engine.dialect.name)
        async with engine.begin() as connection:
            await connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": MIGRATION_LOCK})
            await connection.run_sync(upgrade_connection)
        from .node_credentials import NodeCredentials
        from .nodes import NodeRegistry

        self.session_factory = deps.session_factory
        self.credentials = NodeCredentials(deps.session_factory)
        self.nodes = NodeRegistry(deps.session_factory)
        self.ready = True

    def bind_tracking(self, reader):
        if self.jobs is not None:
            raise RuntimeError("Fleet tracking already bound")
        from .job_service import FleetJobService
        from .mcp_driver import FleetTaskDriver
        from .reconcile import JobReconciler

        self.jobs = FleetJobService(self.session_factory, self.config, tracking_reader=reader)
        self.reconciler = JobReconciler(self.jobs)
        self.reconciler.start()
        return FleetTaskDriver(self.jobs)

    async def stop(self) -> None:
        self.ready = False
        if self.reconciler is not None:
            await self.reconciler.stop()
            self.reconciler = None
        self.jobs = None
        self.session_factory = None
        self.credentials = None
        self.nodes = None
