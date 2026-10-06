"""Immutable original scheduled aggregate association and terminal resolution."""

import sqlalchemy as sa
from alembic import op

revision = "f0017_scheduled_agent_tasks"
down_revision = "f0016_task_budgets"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "fleet_scheduled_agent_tasks",
        sa.Column("occurrence_id", sa.String(64), primary_key=True),
        *(sa.Column(name, sa.String(64), nullable=False) for name in ("scheduled_task_id", "user_id", "thread_id", "original_run_id")),
        sa.Column("agent_task_id", sa.String(64), sa.ForeignKey("fleet_agent_tasks.id"), nullable=False),
        sa.Column("source_generation", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("resolution_kind", sa.String(16)),
        sa.Column("resolved_run_id", sa.String(64)),
        sa.Column("resolved_generation", sa.Integer()),
        sa.Column("resolved_attempt_id", sa.String(64), sa.ForeignKey("fleet_attempts.id")),
        sa.Column("resolved_node_session_id", sa.String(64)),
        sa.Column("resolved_workspace_point_id", sa.String(64), sa.ForeignKey("fleet_workspace_points.id")),
        sa.Column("resolved_operation_id", sa.String(64), sa.ForeignKey("fleet_task_operation_receipts.id")),
        sa.Column("resolved_status", sa.String(24)),
        sa.Column("resolved_error", sa.Text()),
        sa.Column("resolved_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("clock_timestamp()")),
        sa.UniqueConstraint("original_run_id", name="uq_fleet_scheduled_agent_original_run"),
        sa.CheckConstraint("source_generation > 0 AND state IN ('pending','resolved')", name="ck_fleet_scheduled_agent_identity"),
        sa.CheckConstraint(
            "(state='pending' AND resolution_kind IS NULL AND resolved_run_id IS NULL AND resolved_generation IS NULL "
            "AND resolved_attempt_id IS NULL AND resolved_node_session_id IS NULL AND resolved_workspace_point_id IS NULL "
            "AND resolved_operation_id IS NULL AND resolved_status IS NULL AND resolved_error IS NULL AND resolved_at IS NULL) OR "
            "(state='resolved' AND resolution_kind IS NOT NULL AND resolved_run_id IS NOT NULL AND resolved_generation IS NOT NULL AND resolved_generation >= source_generation "
            "AND resolved_status IS NOT NULL AND resolved_status IN ('succeeded','failed','cancelled','timed_out') AND resolved_at IS NOT NULL AND "
            "((resolution_kind='stopped' AND resolved_operation_id IS NULL AND resolved_attempt_id IS NOT NULL AND resolved_node_session_id IS NOT NULL AND resolved_workspace_point_id IS NOT NULL) OR "
            "(resolution_kind='cancelled' AND resolved_status='cancelled' AND resolved_operation_id IS NOT NULL AND resolved_attempt_id IS NOT NULL AND resolved_node_session_id IS NOT NULL AND resolved_workspace_point_id IS NOT NULL) OR "
            "(resolution_kind='unassigned' AND resolved_operation_id IS NULL AND resolved_status='cancelled' AND resolved_attempt_id IS NULL AND resolved_node_session_id IS NULL AND resolved_workspace_point_id IS NULL)))",
            name="ck_fleet_scheduled_agent_resolution",
        ),
    )
    op.create_index("ix_fleet_scheduled_agent_pending", "fleet_scheduled_agent_tasks", ["scheduled_task_id", "state", "occurrence_id"])
    # Core scheduler tables may not exist when this optional extension starts.
    # Historical identity/proof repair belongs to trusted runtime reconciliation.
    op.execute("""
        CREATE FUNCTION fleet_scheduled_agent_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP='DELETE' THEN
                RAISE EXCEPTION 'Scheduled aggregate receipt cannot be deleted';
            END IF;
            IF (NEW.occurrence_id,NEW.scheduled_task_id,NEW.user_id,NEW.thread_id,NEW.original_run_id,NEW.agent_task_id,NEW.source_generation,NEW.created_at)
                IS DISTINCT FROM (OLD.occurrence_id,OLD.scheduled_task_id,OLD.user_id,OLD.thread_id,OLD.original_run_id,OLD.agent_task_id,OLD.source_generation,OLD.created_at)
                OR OLD.state='resolved' AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
                RAISE EXCEPTION 'Scheduled aggregate identity/resolution cannot change';
            END IF;
            RETURN NEW;
        END $$
    """)
    op.execute("CREATE TRIGGER fleet_scheduled_agent_immutable BEFORE UPDATE OR DELETE ON fleet_scheduled_agent_tasks FOR EACH ROW EXECUTE FUNCTION fleet_scheduled_agent_immutable()")


def downgrade():
    raise RuntimeError("Cannot discard original scheduled aggregate resolution receipts")
