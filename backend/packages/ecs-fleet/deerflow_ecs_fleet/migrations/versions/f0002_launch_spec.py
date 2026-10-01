"""Persist the exact operator profile authorized when capacity was reserved."""

from alembic import op

revision = "f0002_launch_spec"
down_revision = "f0001_jobs"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TABLE fleet_attempts ADD COLUMN launch_spec JSONB")


def downgrade():
    raise RuntimeError("Fleet downgrade requires drained execution and a backup")
