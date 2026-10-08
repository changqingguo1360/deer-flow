"""Frozen initial Fleet-owned schema. Never consult mutable ORM metadata here."""

from alembic import op

revision = "f0001_jobs"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""CREATE TABLE fleet_jobs (
	id VARCHAR(64) NOT NULL,
	user_id VARCHAR(64) NOT NULL,
	thread_id VARCHAR(64) NOT NULL,
	source_run_id VARCHAR(64),
	tracking_task_id VARCHAR(64) NOT NULL,
	idempotency_key VARCHAR(128) NOT NULL,
	dedupe_group VARCHAR(128),
	spec JSONB NOT NULL,
	state VARCHAR(24) DEFAULT 'staged' NOT NULL,
	active_attempt_id VARCHAR(64),
	accepted_manifest_id VARCHAR(64),
	error TEXT,
	staged_deadline TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	queue_deadline TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	queued_at TIMESTAMP WITH TIME ZONE,
	cancel_requested_at TIMESTAMP WITH TIME ZONE,
	finished_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT uq_fleet_jobs_submission UNIQUE (user_id, idempotency_key),
	CONSTRAINT uq_fleet_jobs_tracking UNIQUE (user_id, tracking_task_id),
	CONSTRAINT ck_fleet_jobs_state CHECK (state IN ('staged','queued','claimed','running','succeeded','failed','cancelled','unknown','quarantined'))
)""")
    op.execute("CREATE INDEX ix_fleet_jobs_due ON fleet_jobs (state, queue_deadline)")
    op.execute("CREATE INDEX ix_fleet_jobs_thread ON fleet_jobs (user_id, thread_id, created_at)")
    op.execute("CREATE UNIQUE INDEX uq_fleet_jobs_active_group ON fleet_jobs (user_id, dedupe_group) WHERE dedupe_group IS NOT NULL AND state IN ('staged','queued','claimed','running','unknown','quarantined')")
    op.execute("""CREATE TABLE fleet_nodes (
	id VARCHAR(64) NOT NULL,
	name VARCHAR(128) NOT NULL,
	admin_state VARCHAR(16) DEFAULT 'enabled' NOT NULL,
	health VARCHAR(16) DEFAULT 'unknown' NOT NULL,
	session_id VARCHAR(64),
	protocol_version INTEGER DEFAULT '1' NOT NULL,
	runtime_digest VARCHAR(128),
	cpu_millis INTEGER NOT NULL,
	memory_mib INTEGER NOT NULL,
	agent_limit INTEGER DEFAULT '0' NOT NULL,
	last_seen_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_fleet_nodes_capacity CHECK (cpu_millis > 0 AND memory_mib > 0 AND agent_limit >= 0),
	CONSTRAINT ck_fleet_nodes_admin CHECK (admin_state IN ('enabled','draining','disabled')),
	CONSTRAINT ck_fleet_nodes_health CHECK (health IN ('online','offline','unknown')),
	UNIQUE (name)
)""")
    op.execute("""CREATE TABLE fleet_attempts (
	id VARCHAR(64) NOT NULL,
	kind VARCHAR(16) NOT NULL,
	job_id VARCHAR(64),
	run_id VARCHAR(64),
	attempt_no INTEGER NOT NULL,
	node_id VARCHAR(64) NOT NULL,
	node_session_id VARCHAR(64) NOT NULL,
	token_hash VARCHAR(64) NOT NULL,
	state VARCHAR(24) DEFAULT 'claimed' NOT NULL,
	process_ref VARCHAR(128),
	output_prefix VARCHAR(512) NOT NULL,
	outcome JSONB,
	lease_expires_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	execution_deadline TIMESTAMP WITH TIME ZONE,
	start_authorized_at TIMESTAMP WITH TIME ZONE,
	started_at TIMESTAMP WITH TIME ZONE,
	stopped_at TIMESTAMP WITH TIME ZONE,
	finished_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_fleet_attempt_identity CHECK ((kind='job' AND job_id IS NOT NULL AND run_id IS NULL) OR (kind='agent' AND run_id IS NOT NULL AND job_id IS NULL)),
	CONSTRAINT ck_fleet_attempt_number CHECK (attempt_no > 0),
	CONSTRAINT ck_fleet_attempt_state CHECK (state IN ('claimed','starting','running','succeeded','failed','cancelled','expired','unknown','quarantined')),
	CONSTRAINT uq_fleet_attempt_job_number UNIQUE (job_id, attempt_no),
	CONSTRAINT uq_fleet_attempt_run_number UNIQUE (run_id, attempt_no),
	FOREIGN KEY(job_id) REFERENCES fleet_jobs (id),
	FOREIGN KEY(node_id) REFERENCES fleet_nodes (id)
)""")
    op.execute("CREATE UNIQUE INDEX uq_fleet_attempt_job_active ON fleet_attempts (job_id) WHERE job_id IS NOT NULL AND state IN ('claimed','starting','running','unknown','quarantined')")
    op.execute("CREATE UNIQUE INDEX uq_fleet_attempt_run_active ON fleet_attempts (run_id) WHERE run_id IS NOT NULL AND state IN ('claimed','starting','running','unknown','quarantined')")
    op.execute("""CREATE TABLE fleet_credentials (
	id VARCHAR(64) NOT NULL,
	node_id VARCHAR(64) NOT NULL,
	token_hash VARCHAR(64) NOT NULL,
	expires_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	revoked_at TIMESTAMP WITH TIME ZONE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	PRIMARY KEY (id),
	FOREIGN KEY(node_id) REFERENCES fleet_nodes (id),
	UNIQUE (token_hash)
)""")
    op.execute("""CREATE TABLE fleet_artifact_manifests (
	id VARCHAR(64) NOT NULL,
	attempt_id VARCHAR(64) NOT NULL,
	user_id VARCHAR(64) NOT NULL,
	thread_id VARCHAR(64) NOT NULL,
	output_prefix VARCHAR(512) NOT NULL,
	files JSONB NOT NULL,
	total_bytes INTEGER NOT NULL,
	sealed_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	PRIMARY KEY (id),
	CONSTRAINT ck_fleet_manifest_bytes CHECK (total_bytes >= 0),
	UNIQUE (attempt_id),
	FOREIGN KEY(attempt_id) REFERENCES fleet_attempts (id)
)""")
    op.execute("""CREATE TABLE fleet_reservations (
	id VARCHAR(64) NOT NULL,
	node_id VARCHAR(64) NOT NULL,
	attempt_id VARCHAR(64) NOT NULL,
	cpu_millis INTEGER NOT NULL,
	memory_mib INTEGER NOT NULL,
	agent_units INTEGER DEFAULT '0' NOT NULL,
	state VARCHAR(24) DEFAULT 'reserved' NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	released_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (id),
	CONSTRAINT ck_fleet_reservation_capacity CHECK (cpu_millis > 0 AND memory_mib > 0 AND agent_units >= 0),
	CONSTRAINT ck_fleet_reservation_state CHECK (state IN ('reserved','active','quarantined','released')),
	CONSTRAINT ck_fleet_reservation_release CHECK ((state='released') = (released_at IS NOT NULL)),
	FOREIGN KEY(node_id) REFERENCES fleet_nodes (id),
	UNIQUE (attempt_id),
	FOREIGN KEY(attempt_id) REFERENCES fleet_attempts (id)
)""")
    op.execute("CREATE INDEX ix_fleet_reservations_node ON fleet_reservations (node_id, state)")


def downgrade():
    raise RuntimeError("Fleet downgrade is destructive; drain and restore an operator-approved backup instead")
