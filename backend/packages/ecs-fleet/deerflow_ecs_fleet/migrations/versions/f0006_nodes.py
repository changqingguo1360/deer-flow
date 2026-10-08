"""Persist node profile boundaries without discarding pre-existing execution."""

from alembic import op

revision = "f0006_nodes"
down_revision = "f0005_job_invocations"
branch_labels = None
depends_on = None


def upgrade():
    # NULL marks legacy nodes: they retain access to operator-configured job
    # profiles. New registrations persist an explicit list, including trusted
    # CLI defaults; an empty list always permits no new execution.
    op.execute("ALTER TABLE fleet_nodes ADD COLUMN profile_allowlist JSONB")
    op.execute("ALTER TABLE fleet_nodes ADD COLUMN registered_by VARCHAR(64)")
    op.execute("ALTER TABLE fleet_nodes ADD CONSTRAINT ck_fleet_nodes_profiles CHECK (profile_allowlist IS NULL OR jsonb_typeof(profile_allowlist) = 'array')")


def downgrade():
    raise RuntimeError("Fleet downgrade requires drained execution and a backup")
