"""Persist public scheduled execution intent."""

import sqlalchemy as sa

revision = "0019_scheduled_execution"
down_revision = "0018_thread_execution_bindings"
branch_labels = None
depends_on = None


def upgrade():
    from deerflow.persistence.migrations._helpers import safe_add_column

    safe_add_column("scheduled_tasks", sa.Column("execution", sa.JSON(), nullable=True))


def downgrade():
    from deerflow.persistence.migrations._helpers import safe_drop_column

    safe_drop_column("scheduled_tasks", "execution")
