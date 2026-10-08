"""Remote placement foundation; no remote execution is activated by this schema."""

from alembic import op

revision = "f0007_agents"
down_revision = "f0006_nodes"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE fleet_agent_tasks (
            id VARCHAR(64) PRIMARY KEY, user_id VARCHAR(64) NOT NULL,
            thread_id VARCHAR(64) NOT NULL, state VARCHAR(24) NOT NULL DEFAULT 'queued',
            current_run_id VARCHAR(64), generation INTEGER NOT NULL DEFAULT 1,
            deadline TIMESTAMPTZ NOT NULL, continuation_budget INTEGER NOT NULL,
            wait_group_id VARCHAR(64), cancel_requested_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT uq_fleet_agent_task_owner UNIQUE (id,user_id,thread_id),
            CONSTRAINT ck_fleet_agent_task_budgets CHECK (generation>0 AND continuation_budget>=0),
            CONSTRAINT ck_fleet_agent_task_state CHECK (state IN ('queued','running','waiting_jobs','paused','input_required','unknown','succeeded','failed','cancelled','timed_out'))
        )
    """)
    op.execute("CREATE UNIQUE INDEX uq_fleet_agent_task_active_thread ON fleet_agent_tasks (user_id,thread_id) WHERE state NOT IN ('succeeded','failed','cancelled','timed_out')")
    op.execute("""
        CREATE TABLE fleet_launch_specs (
            id VARCHAR(64) PRIMARY KEY, run_id VARCHAR(64) NOT NULL UNIQUE,
            agent_task_id VARCHAR(64) NOT NULL, generation INTEGER NOT NULL,
            user_id VARCHAR(64) NOT NULL, thread_id VARCHAR(64) NOT NULL,
            payload JSONB NOT NULL, payload_digest VARCHAR(71) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT fk_fleet_launch_spec_owner FOREIGN KEY (agent_task_id,user_id,thread_id) REFERENCES fleet_agent_tasks (id,user_id,thread_id),
            CONSTRAINT uq_fleet_launch_spec_identity UNIQUE (id,run_id,agent_task_id,generation,user_id,thread_id),
            CONSTRAINT ck_fleet_launch_spec_generation CHECK (generation>0),
            CONSTRAINT ck_fleet_launch_spec_digest CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$')
        )
    """)
    op.execute("""
        CREATE TABLE fleet_run_placements (
            run_id VARCHAR(64) PRIMARY KEY, agent_task_id VARCHAR(64) NOT NULL,
            generation INTEGER NOT NULL, user_id VARCHAR(64) NOT NULL,
            thread_id VARCHAR(64) NOT NULL, requested_backend VARCHAR(16) NOT NULL,
            node_id VARCHAR(64) REFERENCES fleet_nodes(id), profile VARCHAR(64) NOT NULL,
            state VARCHAR(24) NOT NULL DEFAULT 'queued',
            active_attempt_id VARCHAR(64) REFERENCES fleet_attempts(id),
            launch_spec_ref VARCHAR(64) NOT NULL UNIQUE, queue_deadline TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT uq_fleet_run_placement_task UNIQUE (run_id,agent_task_id),
            CONSTRAINT fk_fleet_placement_launch_identity FOREIGN KEY (launch_spec_ref,run_id,agent_task_id,generation,user_id,thread_id) REFERENCES fleet_launch_specs (id,run_id,agent_task_id,generation,user_id,thread_id),
            CONSTRAINT ck_fleet_placement_generation CHECK (generation>0),
            CONSTRAINT ck_fleet_placement_backend CHECK (requested_backend IN ('remote','auto')),
            CONSTRAINT ck_fleet_placement_state CHECK (state IN ('queued','claimed','running','unknown','succeeded','failed','cancelled','timed_out'))
        )
    """)
    op.execute("ALTER TABLE fleet_agent_tasks ADD CONSTRAINT fk_fleet_agent_task_current_run FOREIGN KEY (current_run_id,id) REFERENCES fleet_run_placements (run_id,agent_task_id) DEFERRABLE INITIALLY DEFERRED")
    op.execute("""
        CREATE FUNCTION fleet_reject_launch_spec_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            RAISE EXCEPTION 'Fleet launch specifications are immutable';
        END $$
    """)
    op.execute("CREATE TRIGGER fleet_launch_specs_immutable BEFORE UPDATE OR DELETE ON fleet_launch_specs FOR EACH ROW EXECUTE FUNCTION fleet_reject_launch_spec_mutation()")


def downgrade():
    raise RuntimeError("Fleet downgrade requires drained execution and a backup")
