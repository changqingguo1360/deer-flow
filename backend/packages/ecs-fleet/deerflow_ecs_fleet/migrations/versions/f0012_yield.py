"""Allow the exact successful final workspace pair to carry waiting dependencies."""

from alembic import op

revision = "f0012_yield"
down_revision = "f0011_continuations"
branch_labels = None
depends_on = None


def upgrade():
    outcome = """(kind='partial' AND desired_core_status IS NULL AND desired_task_status IS NULL AND desired_placement_status IS NULL AND error IS NULL AND stop_reason IS NULL) OR
(kind IN ('final','paused') AND desired_core_status IS NOT NULL AND desired_task_status IS NOT NULL AND desired_placement_status IS NOT NULL
 AND desired_core_status IN ('success','error','interrupted','timeout') AND desired_task_status IN ('succeeded','failed','cancelled','timed_out','paused','input_required','waiting_jobs')
 AND desired_placement_status IN ('succeeded','failed','cancelled','timed_out')
 AND (desired_task_status!='waiting_jobs' OR (kind='final' AND desired_core_status='success' AND desired_placement_status='succeeded' AND error IS NULL)))"""
    for table, constraint in (("fleet_workspace_requests", "ck_fleet_workspace_request_outcome"), ("fleet_workspace_points", "ck_fleet_workspace_point_outcome")):
        op.drop_constraint(constraint, table, type_="check")
        op.create_check_constraint(constraint, table, outcome)


def downgrade():
    raise RuntimeError("Cannot discard durable yielded dependency outcomes")
