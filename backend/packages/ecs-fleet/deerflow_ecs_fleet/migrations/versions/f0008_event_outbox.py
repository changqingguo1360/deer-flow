"""Committed remote transport pointers and immutable stream closure receipts."""

from alembic import op

revision = "f0008_event_outbox"
down_revision = "f0007_agents"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE fleet_event_outbox (
            run_id VARCHAR(64) NOT NULL REFERENCES fleet_run_placements(run_id),
            attempt_id VARCHAR(64) NOT NULL REFERENCES fleet_attempts(id),
            generation INTEGER NOT NULL, user_id VARCHAR(64) NOT NULL,
            thread_id VARCHAR(64) NOT NULL, launch_spec_digest VARCHAR(71) NOT NULL,
            event_id BIGINT NOT NULL UNIQUE, seq BIGINT NOT NULL,
            published_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (run_id,attempt_id,seq),
            CONSTRAINT uq_fleet_event_outbox_thread_seq UNIQUE (thread_id,seq),
            CONSTRAINT ck_fleet_event_outbox_sequence CHECK (generation>0 AND seq>0)
        )
    """)
    op.execute("CREATE INDEX ix_fleet_event_outbox_pending ON fleet_event_outbox (run_id,attempt_id,seq) WHERE published_at IS NULL")
    op.execute("""
        CREATE TABLE fleet_stream_seals (
            run_id VARCHAR(64) NOT NULL REFERENCES fleet_run_placements(run_id),
            attempt_id VARCHAR(64) NOT NULL REFERENCES fleet_attempts(id),
            generation INTEGER NOT NULL, user_id VARCHAR(64) NOT NULL,
            thread_id VARCHAR(64) NOT NULL, launch_spec_digest VARCHAR(71) NOT NULL,
            last_seq BIGINT NOT NULL, core_status VARCHAR(16) NOT NULL,
            source VARCHAR(16) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (run_id,attempt_id),
            CONSTRAINT ck_fleet_stream_seal_sequence CHECK (generation>0 AND last_seq>=0),
            CONSTRAINT ck_fleet_stream_seal_status CHECK (core_status IN ('success','error','interrupted','timeout')),
            CONSTRAINT ck_fleet_stream_seal_source CHECK (source IN ('writer','physical_stop'))
        )
    """)
    op.execute("""
        CREATE FUNCTION fleet_reject_stream_identity_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            IF TG_TABLE_NAME='fleet_event_outbox' AND
               (to_jsonb(NEW)-'published_at') = (to_jsonb(OLD)-'published_at') THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'Fleet committed stream identities are immutable';
        END $$
    """)
    for table in ("fleet_event_outbox", "fleet_stream_seals"):
        op.execute(f"CREATE TRIGGER {table}_immutable BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION fleet_reject_stream_identity_mutation()")


def downgrade():
    raise RuntimeError("Fleet downgrade requires drained execution and a backup")
