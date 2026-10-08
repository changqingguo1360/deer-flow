"""Persistent exactly-one group admission receipt."""

import sqlalchemy as sa
from alembic import op

revision = "f0013_continuation_receipt"
down_revision = "f0012_yield"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("fleet_wait_groups", sa.Column("continuation_run_id", sa.String(64), nullable=True))
    op.add_column("fleet_wait_groups", sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True))
    op.create_foreign_key("fk_fleet_wait_group_continuation_run", "fleet_wait_groups", "fleet_run_placements", ["continuation_run_id"], ["run_id"])
    op.create_unique_constraint("uq_fleet_wait_group_continuation_run", "fleet_wait_groups", ["continuation_run_id"])


def downgrade():
    raise RuntimeError("Cannot discard committed continuation receipts")
