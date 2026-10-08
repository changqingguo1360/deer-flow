"""Run only with the transaction and migration lock supplied by FleetService."""

from alembic import context

config = context.config
connection = config.attributes.get("connection")
if connection is None:
    raise RuntimeError("Fleet migrations require a locked host database connection")
context.configure(connection=connection, version_table="fleet_alembic_version")
with context.begin_transaction():
    context.run_migrations()
