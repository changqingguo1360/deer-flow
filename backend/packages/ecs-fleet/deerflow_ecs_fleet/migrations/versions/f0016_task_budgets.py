"""Frozen lifetime limits and immutable admission/model reservation receipts."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "f0016_task_budgets"
down_revision = "f0015_task_operation_receipts"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "fleet_task_budgets",
        sa.Column("agent_task_id", sa.String(64), sa.ForeignKey("fleet_agent_tasks.id"), primary_key=True),
        *(sa.Column(name, sa.BigInteger(), nullable=False) for name in ("run_limit", "job_limit", "token_limit")),
        *(sa.Column(name, sa.BigInteger(), nullable=False, server_default="0") for name in ("admitted_runs", "submitted_jobs", "spent_tokens", "reserved_tokens")),
        sa.Column("legacy_usage_unknown", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("blocked_reason", sa.String(128)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("clock_timestamp()")),
        sa.CheckConstraint(
            "run_limit >= 0 AND job_limit >= 0 AND token_limit >= 0 AND admitted_runs >= 0 AND submitted_jobs >= 0 "
            "AND spent_tokens >= 0 AND reserved_tokens >= 0 AND admitted_runs <= run_limit AND submitted_jobs <= job_limit "
            "AND spent_tokens + reserved_tokens <= token_limit",
            name="ck_fleet_task_budget_limits",
        ),
    )
    op.create_table(
        "fleet_task_budget_charges",
        sa.Column("agent_task_id", sa.String(64), sa.ForeignKey("fleet_task_budgets.agent_task_id"), primary_key=True),
        sa.Column("kind", sa.String(8), primary_key=True),
        sa.Column("logical_id", sa.String(64), primary_key=True),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("clock_timestamp()")),
        sa.CheckConstraint("kind IN ('run','job') AND generation > 0", name="ck_fleet_task_budget_charge"),
    )
    op.create_table(
        "fleet_model_reservations",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("agent_task_id", sa.String(64), sa.ForeignKey("fleet_task_budgets.agent_task_id"), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("attempt_id", sa.String(64), sa.ForeignKey("fleet_attempts.id"), nullable=False),
        *(sa.Column(name, sa.String(64), nullable=False) for name in ("node_id", "node_session_id", "token_stamp", "request_digest")),
        sa.Column("provider_contract", JSONB(), nullable=False),
        *(sa.Column(name, sa.BigInteger(), nullable=False) for name in ("input_bound", "output_bound", "reserved_tokens")),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("measured_usage", JSONB()),
        sa.Column("unknown_reason", sa.String(128)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("clock_timestamp()")),
        sa.CheckConstraint("state IN ('reserved','settled','unknown') AND generation > 0 AND input_bound > 0 AND output_bound > 0 AND reserved_tokens = input_bound + output_bound", name="ck_fleet_model_reservation"),
    )
    op.create_index("ix_fleet_model_reservations_task", "fleet_model_reservations", ["agent_task_id", "state"])
    op.create_table(
        "fleet_task_budget_decisions",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("agent_task_id", sa.String(64), sa.ForeignKey("fleet_task_budgets.agent_task_id"), nullable=False),
        *(sa.Column(name, sa.String(64), nullable=False) for name in ("user_id", "thread_id")),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("source_run_id", sa.String(64)),
        sa.Column("request_digest", sa.String(64)),
        sa.Column("reason", sa.String(128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("clock_timestamp()")),
    )
    # Historic tokens cannot be reconstructed from completed responses. Existing
    # goals get zero new token allowance, with publication identity untouched.
    op.execute("""
        INSERT INTO fleet_task_budgets(agent_task_id,run_limit,job_limit,token_limit,admitted_runs,submitted_jobs,legacy_usage_unknown,blocked_reason)
        SELECT t.id,(SELECT count(*) FROM fleet_run_placements p WHERE p.agent_task_id=t.id),
            (SELECT count(*) FROM fleet_job_links l WHERE l.agent_task_id=t.id),0,
            (SELECT count(*) FROM fleet_run_placements p WHERE p.agent_task_id=t.id),
            (SELECT count(*) FROM fleet_job_links l WHERE l.agent_task_id=t.id),true,'legacy_usage_unknown'
        FROM fleet_agent_tasks t
    """)
    op.execute("""
        INSERT INTO fleet_task_budget_charges(agent_task_id,kind,logical_id,generation)
        SELECT agent_task_id,'run',run_id,generation FROM fleet_run_placements
        UNION ALL SELECT agent_task_id,'job',job_id,generation FROM fleet_job_links
    """)
    # Frozen limits and original reservation metadata cannot be rewritten by a
    # later generation. Settlement is a one-way immutable receipt transition.
    op.execute("""
        CREATE FUNCTION fleet_budget_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_TABLE_NAME='fleet_task_budgets' THEN
                IF (NEW.agent_task_id,NEW.run_limit,NEW.job_limit,NEW.token_limit,NEW.legacy_usage_unknown)
                    IS DISTINCT FROM (OLD.agent_task_id,OLD.run_limit,OLD.job_limit,OLD.token_limit,OLD.legacy_usage_unknown)
                    OR NEW.spent_tokens < OLD.spent_tokens OR NEW.admitted_runs < OLD.admitted_runs OR NEW.submitted_jobs < OLD.submitted_jobs THEN
                    RAISE EXCEPTION 'Frozen task budget identity/charges cannot change';
                END IF;
            ELSIF TG_TABLE_NAME='fleet_model_reservations' THEN
                IF (to_jsonb(NEW)-'state'-'measured_usage'-'unknown_reason') IS DISTINCT FROM (to_jsonb(OLD)-'state'-'measured_usage'-'unknown_reason')
                    OR OLD.state='settled' AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD)
                    OR OLD.state='unknown' AND NEW.state <> 'unknown' THEN
                    RAISE EXCEPTION 'Immutable model reservation receipt cannot change';
                END IF;
            ELSE RAISE EXCEPTION 'Immutable task admission receipt cannot change';
            END IF;
            RETURN NEW;
        END $$
    """)
    for table in ("fleet_task_budgets", "fleet_model_reservations", "fleet_task_budget_charges", "fleet_task_budget_decisions"):
        op.execute(f"CREATE TRIGGER fleet_budget_immutable BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION fleet_budget_immutable()")


def downgrade():
    raise RuntimeError("Cannot discard cumulative task charges and uncertain model usage")
