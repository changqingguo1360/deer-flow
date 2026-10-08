"""Persist immutable upload versions independently of execution attempts."""

from alembic import op

revision = "f0003_inputs"
down_revision = "f0002_launch_spec"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE fleet_input_manifests (
            id VARCHAR(64) PRIMARY KEY,
            user_id VARCHAR(64) NOT NULL,
            thread_id VARCHAR(64) NOT NULL,
            output_prefix VARCHAR(512) NOT NULL UNIQUE,
            files JSONB NOT NULL,
            total_bytes INTEGER NOT NULL,
            sealed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT ck_fleet_input_bytes CHECK (total_bytes >= 0)
        )
    """)
    op.execute("CREATE INDEX ix_fleet_inputs_owner ON fleet_input_manifests (user_id, thread_id, sealed_at)")


def downgrade():
    raise RuntimeError("Fleet downgrade requires drained execution and a backup")
