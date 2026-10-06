"""Durable shared admission turn and incarnation-scoped worker capabilities."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "f0014_shared_scheduling"
down_revision = "f0013_continuation_receipt"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("fleet_nodes", sa.Column("claim_kinds", JSONB(), nullable=True))
    op.create_table(
        "fleet_scheduling",
        sa.Column("id", sa.String(16), primary_key=True),
        sa.Column("next_kind", sa.String(16), nullable=False, server_default="job"),
        sa.CheckConstraint("id='shared' AND next_kind IN ('job','agent')", name="ck_fleet_scheduling_turn"),
    )
    op.execute("INSERT INTO fleet_scheduling(id,next_kind) VALUES ('shared','job')")


def downgrade():
    raise RuntimeError("Cannot discard durable scheduling fairness")
