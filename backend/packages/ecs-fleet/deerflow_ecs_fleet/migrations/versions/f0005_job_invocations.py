"""Freeze every submission decision, including unfinished-work reuse."""

from alembic import op

revision = "f0005_job_invocations"
down_revision = "f0004_recovery"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE fleet_job_invocations (
            user_id VARCHAR(64) NOT NULL,
            idempotency_key VARCHAR(128) NOT NULL,
            thread_id VARCHAR(64) NOT NULL,
            source_run_id VARCHAR(64),
            spec JSONB NOT NULL,
            dedupe_group VARCHAR(128),
            job_id VARCHAR(64) NOT NULL REFERENCES fleet_jobs(id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (user_id, idempotency_key)
        )
    """)
    op.execute("CREATE INDEX ix_fleet_job_invocations_job ON fleet_job_invocations (job_id)")
    op.execute("""
        INSERT INTO fleet_job_invocations
            (user_id, idempotency_key, thread_id, source_run_id, spec, dedupe_group, job_id, created_at)
        SELECT user_id, idempotency_key, thread_id, source_run_id, spec, dedupe_group, id, created_at
        FROM fleet_jobs
    """)


def downgrade():
    raise RuntimeError("Fleet downgrade requires drained execution and a backup")
