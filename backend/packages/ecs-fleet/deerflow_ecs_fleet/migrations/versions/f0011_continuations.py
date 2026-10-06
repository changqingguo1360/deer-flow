"""Immutable original child ownership and sealed dependency membership."""

from alembic import op

revision = "f0011_continuations"
down_revision = "f0010_scheduler_tickets"
branch_labels = None
depends_on = None
DDL = (
    "ALTER TABLE fleet_launch_specs ADD CONSTRAINT uq_fleet_launch_parent UNIQUE (run_id,agent_task_id,generation,user_id,thread_id)",
    "ALTER TABLE fleet_jobs ADD CONSTRAINT uq_fleet_job_parent UNIQUE (id,user_id,thread_id,source_run_id)",
    """CREATE TABLE fleet_job_links (
 job_id VARCHAR(64) PRIMARY KEY, agent_task_id VARCHAR(64) NOT NULL,
 generation INTEGER NOT NULL CHECK (generation > 0), parent_run_id VARCHAR(64) NOT NULL,
 user_id VARCHAR(64) NOT NULL, thread_id VARCHAR(64) NOT NULL,
 link_mode VARCHAR(16) NOT NULL CHECK (link_mode IN ('awaited','detached')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE (agent_task_id,generation,job_id),
 FOREIGN KEY (parent_run_id,agent_task_id,generation,user_id,thread_id)
 REFERENCES fleet_launch_specs(run_id,agent_task_id,generation,user_id,thread_id),
 FOREIGN KEY (job_id,user_id,thread_id,parent_run_id)
 REFERENCES fleet_jobs(id,user_id,thread_id,source_run_id))""",
    """CREATE TABLE fleet_wait_groups (
 id VARCHAR(64) PRIMARY KEY, continuation_key VARCHAR(128) NOT NULL UNIQUE,
 agent_task_id VARCHAR(64) NOT NULL, generation INTEGER NOT NULL CHECK (generation > 0),
 parent_run_id VARCHAR(64) NOT NULL, user_id VARCHAR(64) NOT NULL, thread_id VARCHAR(64) NOT NULL,
 job_ids JSONB NOT NULL CHECK (jsonb_typeof(job_ids)='array' AND jsonb_array_length(job_ids)>0),
 policy VARCHAR(24) NOT NULL DEFAULT 'all_settled' CHECK (policy='all_settled'),
 state VARCHAR(24) NOT NULL DEFAULT 'sealed', checkpoint_id VARCHAR(64),
 workspace_point_id VARCHAR(64), delivery_owner VARCHAR(128),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY (parent_run_id,agent_task_id,generation,user_id,thread_id)
 REFERENCES fleet_launch_specs(run_id,agent_task_id,generation,user_id,thread_id))""",
    """CREATE FUNCTION fleet_reject_link_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION 'Fleet child ownership is immutable'; END $$""",
    "CREATE TRIGGER fleet_job_link_immutable BEFORE UPDATE OR DELETE ON fleet_job_links FOR EACH ROW EXECUTE FUNCTION fleet_reject_link_mutation()",
    """CREATE FUNCTION fleet_reject_group_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Fleet sealed group is immutable'; END IF;
 IF ROW(NEW.id,NEW.continuation_key,NEW.agent_task_id,NEW.generation,NEW.parent_run_id,NEW.user_id,NEW.thread_id,NEW.job_ids,NEW.policy,NEW.created_at)
 IS DISTINCT FROM ROW(OLD.id,OLD.continuation_key,OLD.agent_task_id,OLD.generation,OLD.parent_run_id,OLD.user_id,OLD.thread_id,OLD.job_ids,OLD.policy,OLD.created_at)
 THEN RAISE EXCEPTION 'Fleet sealed membership is immutable'; END IF;
 RETURN NEW; END $$""",
    "CREATE TRIGGER fleet_wait_group_immutable BEFORE UPDATE OR DELETE ON fleet_wait_groups FOR EACH ROW EXECUTE FUNCTION fleet_reject_group_mutation()",
)


def upgrade():
    for statement in DDL:
        op.execute(statement)


def downgrade():
    raise RuntimeError("Cannot discard durable child ownership or sealed wait groups")
