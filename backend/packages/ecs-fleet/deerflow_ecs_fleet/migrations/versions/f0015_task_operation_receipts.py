"""Private durable trusted user operation authorization."""

import sqlalchemy as sa
from alembic import op

revision = "f0015_task_operation_receipts"
down_revision = "f0014_shared_scheduling"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "fleet_task_operation_receipts",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("agent_task_id", sa.String(64), sa.ForeignKey("fleet_agent_tasks.id"), nullable=False),
        sa.Column("user_id", sa.String(64), nullable=False),
        sa.Column("thread_id", sa.String(64), nullable=False),
        sa.Column("operation", sa.String(32), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("request_digest", sa.String(64)),
        sa.Column("source_point_generation", sa.Integer()),
        sa.Column("preceding_receipt_id", sa.String(64)),
        sa.Column("stopped_run_id", sa.String(64)),
        sa.Column("source_generation", sa.Integer(), nullable=False),
        sa.Column("target_generation", sa.Integer(), nullable=False),
        sa.Column("source_run_id", sa.String(64)),
        sa.Column("source_workspace_point_id", sa.String(128)),
        sa.Column("source_checkpoint_id", sa.String(128)),
        sa.Column("wait_group_id", sa.String(64)),
        sa.Column("admitted_run_id", sa.String(64), unique=True),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("clock_timestamp()")),
        sa.UniqueConstraint("agent_task_id", "operation", "idempotency_key", name="uq_fleet_task_operation_key"),
        sa.CheckConstraint("source_generation > 0 AND target_generation >= source_generation", name="ck_fleet_task_operation_generation"),
    )


def downgrade():
    raise RuntimeError("Cannot discard durable task operation receipts")
