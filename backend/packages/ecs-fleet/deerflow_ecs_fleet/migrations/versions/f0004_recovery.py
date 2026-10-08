"""Record operator resolution without discarding uncertain execution history."""

from alembic import op

revision = "f0004_recovery"
down_revision = "f0003_inputs"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE fleet_recovery_events (
            id VARCHAR(64) PRIMARY KEY,
            job_id VARCHAR(64) NOT NULL REFERENCES fleet_jobs(id),
            attempt_id VARCHAR(64) NOT NULL UNIQUE REFERENCES fleet_attempts(id),
            operator_id VARCHAR(64) NOT NULL,
            action VARCHAR(32) NOT NULL,
            note TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT ck_fleet_recovery_action CHECK (action = 'fail_stopped')
        )
    """)
    op.execute("CREATE INDEX ix_fleet_recovery_job ON fleet_recovery_events (job_id, created_at)")


def downgrade():
    raise RuntimeError("Fleet downgrade requires drained execution and a backup")
