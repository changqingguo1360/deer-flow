"""Prelaunch capacity uses the original resource ledger."""

from alembic import op

revision = "f0010_scheduler_tickets"
down_revision = "f0009_workspace_points"
branch_labels = None
depends_on = None
DDL = (
    "ALTER TABLE fleet_nodes ADD COLUMN agent_compatibility JSONB",
    """CREATE TABLE fleet_scheduler_tickets (
 id VARCHAR(64) PRIMARY KEY, occurrence_id VARCHAR(64) NOT NULL,
 scheduled_task_id VARCHAR(64) NOT NULL, user_id VARCHAR(64) NOT NULL,
 thread_id VARCHAR(64) NOT NULL, profile VARCHAR(64) NOT NULL,
 node_id VARCHAR(64) NOT NULL REFERENCES fleet_nodes(id), node_session_id VARCHAR(64) NOT NULL,
 lease_owner VARCHAR(128) NOT NULL, run_id VARCHAR(64),
 state VARCHAR(24) NOT NULL DEFAULT 'held' CHECK (state IN ('held','consumed','released')),
 expires_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 consumed_at TIMESTAMPTZ, released_at TIMESTAMPTZ)""",
    "CREATE UNIQUE INDEX uq_fleet_ticket_live_occurrence ON fleet_scheduler_tickets(occurrence_id) WHERE state != 'released'",
    "ALTER TABLE fleet_reservations ALTER COLUMN attempt_id DROP NOT NULL",
    "ALTER TABLE fleet_reservations ADD COLUMN ticket_id VARCHAR(64) UNIQUE REFERENCES fleet_scheduler_tickets(id)",
    "ALTER TABLE fleet_reservations ADD CONSTRAINT ck_fleet_reservation_identity CHECK (attempt_id IS NOT NULL OR ticket_id IS NOT NULL)",
)


def upgrade():
    for statement in DDL:
        op.execute(statement)


def downgrade():
    raise RuntimeError("Ticket reservation downgrade cannot discard accepted capacity")
