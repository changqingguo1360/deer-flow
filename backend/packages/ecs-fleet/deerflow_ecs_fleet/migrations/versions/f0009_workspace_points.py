"""Immutable private C workspace boundary foundation; no lifecycle activation."""

from alembic import op

revision = "f0009_workspace_points"
down_revision = "f0008_event_outbox"
branch_labels = None
depends_on = None

# Frozen DDL: historical migration never imports mutable application metadata.
DDL = (
    """
    ALTER TABLE fleet_attempts ADD CONSTRAINT uq_fleet_attempt_run_identity UNIQUE (id, run_id)
    """,
    """
    ALTER TABLE fleet_attempts ADD CONSTRAINT uq_fleet_attempt_original_execution UNIQUE (id, run_id, node_id, node_session_id, token_hash, process_ref)
    """,
    """
    ALTER TABLE fleet_run_placements ADD CONSTRAINT uq_fleet_placement_workspace_owner UNIQUE (run_id, agent_task_id, generation, user_id, thread_id)
    """,
    """
    ALTER TABLE fleet_launch_specs ADD CONSTRAINT uq_fleet_launch_workspace_digest UNIQUE (run_id, agent_task_id, generation, user_id, thread_id, payload_digest)
    """,
    """
    ALTER TABLE fleet_agent_tasks ADD COLUMN accepted_workspace_point_id VARCHAR(64)
    """,
    """
    ALTER TABLE fleet_run_placements ADD COLUMN final_workspace_point_id VARCHAR(64)
    """,
    """
    ALTER TABLE fleet_agent_tasks DROP CONSTRAINT ck_fleet_agent_task_state
    """,
    """
    ALTER TABLE fleet_agent_tasks ADD CONSTRAINT ck_fleet_agent_task_state CHECK (state IN
    ('queued','running','waiting_jobs','paused','input_required','unknown','finishing','recovery_required','succeeded','failed','cancelled','timed_out'))
    """,
    """
    ALTER TABLE fleet_run_placements DROP CONSTRAINT ck_fleet_placement_state
    """,
    """
    ALTER TABLE fleet_run_placements ADD CONSTRAINT ck_fleet_placement_state CHECK (state IN ('queued','claimed','running','unknown','finishing','recovery_required','succeeded','failed','cancelled','timed_out'))
    """,
    """
    CREATE TABLE fleet_workspace_requests (
        id VARCHAR(64) NOT NULL,
        run_id VARCHAR(64) NOT NULL,
        agent_task_id VARCHAR(64) NOT NULL,
        generation INTEGER NOT NULL,
        user_id VARCHAR(64) NOT NULL,
        thread_id VARCHAR(64) NOT NULL,
        attempt_id VARCHAR(64) NOT NULL,
        launch_spec_digest VARCHAR(71) NOT NULL,
        node_id VARCHAR(64) NOT NULL,
        node_session_id VARCHAR(64) NOT NULL,
        token_stamp VARCHAR(64) NOT NULL,
        process_ref VARCHAR(128) NOT NULL,
        owner_worker_id VARCHAR(128) NOT NULL,
        request_digest VARCHAR(64) NOT NULL,
        checkpoint_ns VARCHAR(64) DEFAULT '' NOT NULL,
        checkpoint_id VARCHAR(128) NOT NULL,
        kind VARCHAR(16) NOT NULL,
        publication_key VARCHAR(128) NOT NULL,
        presented_paths JSONB NOT NULL,
        source_workspace_version VARCHAR(128) NOT NULL,
        desired_core_status VARCHAR(16),
        desired_task_status VARCHAR(24),
        desired_placement_status VARCHAR(24),
        error TEXT,
        stop_reason VARCHAR(128),
        state VARCHAR(16) DEFAULT 'requested' NOT NULL,
        claim_nonce VARCHAR(64),
        claim_lease_expires_at TIMESTAMP WITH TIME ZONE,
        barrier_epoch BIGINT DEFAULT '0' NOT NULL,
        candidate_manifest_id VARCHAR(64),
        rejection TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT fk_fleet_workspace_request_placement FOREIGN KEY(run_id, agent_task_id, generation, user_id, thread_id) REFERENCES fleet_run_placements (run_id, agent_task_id, generation, user_id, thread_id),
        CONSTRAINT fk_fleet_workspace_request_launch FOREIGN KEY(run_id, agent_task_id, generation, user_id, thread_id, launch_spec_digest) REFERENCES fleet_launch_specs (run_id, agent_task_id, generation, user_id,
    thread_id, payload_digest),
        CONSTRAINT fk_fleet_workspace_request_attempt FOREIGN KEY(attempt_id, run_id) REFERENCES fleet_attempts (id, run_id),
        CONSTRAINT fk_fleet_workspace_request_execution FOREIGN KEY(attempt_id, run_id, node_id, node_session_id, token_stamp, process_ref) REFERENCES fleet_attempts (id, run_id, node_id, node_session_id,
    token_hash, process_ref),
        CONSTRAINT ck_fleet_workspace_request_owner CHECK (process_ref = 'fleet-' || attempt_id AND generation > 0 AND owner_worker_id = 'fleet-agent:' || attempt_id),
        CONSTRAINT ck_fleet_workspace_request_digests CHECK (launch_spec_digest ~ '^sha256:[a-f0-9]{64}$' AND token_stamp ~ '^[a-f0-9]{64}$'),
        CONSTRAINT uq_fleet_workspace_request_publication UNIQUE (attempt_id, checkpoint_id, kind, publication_key),
        CONSTRAINT uq_fleet_workspace_request_identity UNIQUE (id, run_id, agent_task_id, generation, user_id, thread_id, attempt_id, launch_spec_digest, node_id, node_session_id, token_stamp, process_ref,
    owner_worker_id, request_digest, checkpoint_ns, checkpoint_id, kind, publication_key),
        CONSTRAINT ck_fleet_workspace_request_boundary CHECK (checkpoint_ns = '' AND checkpoint_id <> '' AND publication_key <> '' AND source_workspace_version <> ''),
        CONSTRAINT ck_fleet_workspace_request_state CHECK (kind IN ('partial','final','paused') AND state IN ('requested','sealing','prepared','accepted','rejected')),
        CONSTRAINT ck_fleet_workspace_request_content CHECK (request_digest ~ '^[a-f0-9]{64}$' AND jsonb_typeof(presented_paths) = 'array'),
        CONSTRAINT ck_fleet_workspace_request_claim CHECK (barrier_epoch >= 0 AND (claim_nonce IS NULL) = (claim_lease_expires_at IS NULL)),
        CONSTRAINT ck_fleet_workspace_request_outcome CHECK ((kind='partial' AND desired_core_status IS NULL AND desired_task_status IS NULL AND desired_placement_status IS NULL AND error IS NULL AND stop_reason IS
    NULL) OR (kind IN ('final','paused') AND desired_core_status IS NOT NULL AND desired_task_status IS NOT NULL AND desired_placement_status IS NOT NULL AND desired_core_status IN
    ('success','error','interrupted','timeout') AND desired_task_status IN ('succeeded','failed','cancelled','timed_out','paused','input_required') AND desired_placement_status IN
    ('succeeded','failed','cancelled','timed_out'))),
        CONSTRAINT uq_fleet_workspace_request_manifest_owner UNIQUE (request_digest, run_id, agent_task_id, generation, user_id, thread_id, attempt_id, launch_spec_digest, node_id, node_session_id, token_stamp,
    process_ref, owner_worker_id)
    )
    """,
    """
    CREATE INDEX ix_fleet_workspace_request_pending ON fleet_workspace_requests (state, claim_lease_expires_at)
    """,
    """
    CREATE TABLE fleet_workspace_manifests (
        id VARCHAR(64) NOT NULL,
        run_id VARCHAR(64) NOT NULL,
        agent_task_id VARCHAR(64) NOT NULL,
        generation INTEGER NOT NULL,
        user_id VARCHAR(64) NOT NULL,
        thread_id VARCHAR(64) NOT NULL,
        attempt_id VARCHAR(64) NOT NULL,
        launch_spec_digest VARCHAR(71) NOT NULL,
        node_id VARCHAR(64) NOT NULL,
        node_session_id VARCHAR(64) NOT NULL,
        token_stamp VARCHAR(64) NOT NULL,
        process_ref VARCHAR(128) NOT NULL,
        owner_worker_id VARCHAR(128) NOT NULL,
        request_digest VARCHAR(64) NOT NULL,
        content_hash VARCHAR(64) NOT NULL,
        schema_version INTEGER DEFAULT '1' NOT NULL,
        categories JSONB NOT NULL,
        directories JSONB NOT NULL,
        files JSONB NOT NULL,
        total_bytes BIGINT NOT NULL,
        nas_prefix VARCHAR(512) NOT NULL,
        sealed_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT fk_fleet_workspace_manifest_placement FOREIGN KEY(run_id, agent_task_id, generation, user_id, thread_id) REFERENCES fleet_run_placements (run_id, agent_task_id, generation, user_id, thread_id),
        CONSTRAINT fk_fleet_workspace_manifest_launch FOREIGN KEY(run_id, agent_task_id, generation, user_id, thread_id, launch_spec_digest) REFERENCES fleet_launch_specs (run_id, agent_task_id, generation,
    user_id, thread_id, payload_digest),
        CONSTRAINT fk_fleet_workspace_manifest_attempt FOREIGN KEY(attempt_id, run_id) REFERENCES fleet_attempts (id, run_id),
        CONSTRAINT fk_fleet_workspace_manifest_execution FOREIGN KEY(attempt_id, run_id, node_id, node_session_id, token_stamp, process_ref) REFERENCES fleet_attempts (id, run_id, node_id, node_session_id,
    token_hash, process_ref),
        CONSTRAINT ck_fleet_workspace_manifest_owner CHECK (process_ref = 'fleet-' || attempt_id AND generation > 0 AND owner_worker_id = 'fleet-agent:' || attempt_id),
        CONSTRAINT ck_fleet_workspace_manifest_digests CHECK (launch_spec_digest ~ '^sha256:[a-f0-9]{64}$' AND token_stamp ~ '^[a-f0-9]{64}$'),
        CONSTRAINT uq_fleet_workspace_manifest_identity UNIQUE (id, run_id, agent_task_id, generation, user_id, thread_id, attempt_id, launch_spec_digest, node_id, node_session_id, token_stamp, process_ref,
    owner_worker_id, request_digest),
        CONSTRAINT ck_fleet_workspace_manifest_content CHECK (schema_version=1 AND total_bytes>=0 AND id=content_hash AND content_hash ~ '^[a-f0-9]{64}$' AND request_digest ~ '^[a-f0-9]{64}$'),
        CONSTRAINT ck_fleet_workspace_manifest_inventory CHECK (categories = '["workspace","uploads","outputs"]'::jsonb AND jsonb_typeof(files)='array' AND jsonb_typeof(directories)='array'),
        UNIQUE (nas_prefix),
        CONSTRAINT fk_fleet_workspace_manifest_request FOREIGN KEY(request_digest, run_id, agent_task_id, generation, user_id, thread_id, attempt_id, launch_spec_digest, node_id, node_session_id, token_stamp,
    process_ref, owner_worker_id) REFERENCES fleet_workspace_requests (request_digest, run_id, agent_task_id, generation, user_id, thread_id, attempt_id, launch_spec_digest, node_id, node_session_id, token_stamp,
    process_ref, owner_worker_id)
    )
    """,
    """
    CREATE TABLE fleet_workspace_points (
        id VARCHAR(64) NOT NULL,
        run_id VARCHAR(64) NOT NULL,
        agent_task_id VARCHAR(64) NOT NULL,
        generation INTEGER NOT NULL,
        user_id VARCHAR(64) NOT NULL,
        thread_id VARCHAR(64) NOT NULL,
        attempt_id VARCHAR(64) NOT NULL,
        launch_spec_digest VARCHAR(71) NOT NULL,
        node_id VARCHAR(64) NOT NULL,
        node_session_id VARCHAR(64) NOT NULL,
        token_stamp VARCHAR(64) NOT NULL,
        process_ref VARCHAR(128) NOT NULL,
        owner_worker_id VARCHAR(128) NOT NULL,
        request_id VARCHAR(64) NOT NULL,
        request_digest VARCHAR(64) NOT NULL,
        checkpoint_ns VARCHAR(64) DEFAULT '' NOT NULL,
        checkpoint_id VARCHAR(128) NOT NULL,
        manifest_id VARCHAR(64) NOT NULL,
        kind VARCHAR(16) NOT NULL,
        publication_key VARCHAR(128) NOT NULL,
        desired_core_status VARCHAR(16),
        desired_task_status VARCHAR(24),
        desired_placement_status VARCHAR(24),
        error TEXT,
        stop_reason VARCHAR(128),
        accepted_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT fk_fleet_workspace_point_placement FOREIGN KEY(run_id, agent_task_id, generation, user_id, thread_id) REFERENCES fleet_run_placements (run_id, agent_task_id, generation, user_id, thread_id),
        CONSTRAINT fk_fleet_workspace_point_launch FOREIGN KEY(run_id, agent_task_id, generation, user_id, thread_id, launch_spec_digest) REFERENCES fleet_launch_specs (run_id, agent_task_id, generation, user_id,
    thread_id, payload_digest),
        CONSTRAINT fk_fleet_workspace_point_attempt FOREIGN KEY(attempt_id, run_id) REFERENCES fleet_attempts (id, run_id),
        CONSTRAINT fk_fleet_workspace_point_execution FOREIGN KEY(attempt_id, run_id, node_id, node_session_id, token_stamp, process_ref) REFERENCES fleet_attempts (id, run_id, node_id, node_session_id, token_hash,
    process_ref),
        CONSTRAINT ck_fleet_workspace_point_owner CHECK (process_ref = 'fleet-' || attempt_id AND generation > 0 AND owner_worker_id = 'fleet-agent:' || attempt_id),
        CONSTRAINT ck_fleet_workspace_point_digests CHECK (launch_spec_digest ~ '^sha256:[a-f0-9]{64}$' AND token_stamp ~ '^[a-f0-9]{64}$'),
        CONSTRAINT uq_fleet_workspace_point_task_owner UNIQUE (id, agent_task_id, user_id, thread_id),
        CONSTRAINT uq_fleet_workspace_point_run_owner UNIQUE (id, run_id, agent_task_id, generation, user_id, thread_id),
        CONSTRAINT fk_fleet_workspace_point_request FOREIGN KEY(request_id, run_id, agent_task_id, generation, user_id, thread_id, attempt_id, launch_spec_digest, node_id, node_session_id, token_stamp, process_ref,
    owner_worker_id, request_digest, checkpoint_ns, checkpoint_id, kind, publication_key) REFERENCES fleet_workspace_requests (id, run_id, agent_task_id, generation, user_id, thread_id, attempt_id,
    launch_spec_digest, node_id, node_session_id, token_stamp, process_ref, owner_worker_id, request_digest, checkpoint_ns, checkpoint_id, kind, publication_key),
        CONSTRAINT fk_fleet_workspace_point_manifest FOREIGN KEY(manifest_id, run_id, agent_task_id, generation, user_id, thread_id, attempt_id, launch_spec_digest, node_id, node_session_id, token_stamp,
    process_ref, owner_worker_id, request_digest) REFERENCES fleet_workspace_manifests (id, run_id, agent_task_id, generation, user_id, thread_id, attempt_id, launch_spec_digest, node_id, node_session_id,
    token_stamp, process_ref, owner_worker_id, request_digest),
        CONSTRAINT ck_fleet_workspace_point_boundary CHECK (checkpoint_ns='' AND kind IN ('partial','final','paused')),
        CONSTRAINT ck_fleet_workspace_point_outcome CHECK ((kind='partial' AND desired_core_status IS NULL AND desired_task_status IS NULL AND desired_placement_status IS NULL AND error IS NULL AND stop_reason IS
    NULL) OR (kind IN ('final','paused') AND desired_core_status IS NOT NULL AND desired_task_status IS NOT NULL AND desired_placement_status IS NOT NULL AND desired_core_status IN
    ('success','error','interrupted','timeout') AND desired_task_status IN ('succeeded','failed','cancelled','timed_out','paused','input_required') AND desired_placement_status IN
    ('succeeded','failed','cancelled','timed_out'))),
        UNIQUE (request_id)
    )
    """,
    """
    CREATE UNIQUE INDEX uq_fleet_workspace_point_final_run ON fleet_workspace_points (run_id) WHERE kind IN ('final','paused')
    """,
    """
    ALTER TABLE fleet_agent_tasks ADD CONSTRAINT fk_fleet_agent_task_workspace_point FOREIGN KEY(accepted_workspace_point_id, id, user_id, thread_id) REFERENCES fleet_workspace_points (id, agent_task_id, user_id,
    thread_id) DEFERRABLE INITIALLY DEFERRED
    """,
    """
    ALTER TABLE fleet_run_placements ADD CONSTRAINT fk_fleet_placement_workspace_point FOREIGN KEY(final_workspace_point_id, run_id, agent_task_id, generation, user_id, thread_id) REFERENCES fleet_workspace_points
    (id, run_id, agent_task_id, generation, user_id, thread_id) DEFERRABLE INITIALLY DEFERRED
    """,
    """
    ALTER TABLE fleet_workspace_requests ADD CONSTRAINT fk_fleet_workspace_request_candidate FOREIGN KEY(candidate_manifest_id, run_id, agent_task_id, generation, user_id, thread_id, attempt_id, launch_spec_digest,
    node_id, node_session_id, token_stamp, process_ref, owner_worker_id, request_digest) REFERENCES fleet_workspace_manifests (id, run_id, agent_task_id, generation, user_id, thread_id, attempt_id,
    launch_spec_digest, node_id, node_session_id, token_stamp, process_ref, owner_worker_id, request_digest) DEFERRABLE INITIALLY DEFERRED
    """,
)


PROCESS_DDL = (
    """
CREATE TABLE fleet_workspace_processes (
	run_id VARCHAR(64) NOT NULL,
	agent_task_id VARCHAR(64) NOT NULL,
	generation INTEGER NOT NULL,
	user_id VARCHAR(64) NOT NULL,
	thread_id VARCHAR(64) NOT NULL,
	attempt_id VARCHAR(64) NOT NULL,
	launch_spec_digest VARCHAR(71) NOT NULL,
	node_id VARCHAR(64) NOT NULL,
	node_session_id VARCHAR(64) NOT NULL,
	token_stamp VARCHAR(64) NOT NULL,
	process_ref VARCHAR(128) NOT NULL,
	owner_worker_id VARCHAR(128) NOT NULL,
	pid INTEGER NOT NULL,
	start_ticks BIGINT NOT NULL,
	role VARCHAR(16) NOT NULL,
	tool_execution_id VARCHAR(64) NOT NULL,
	start_nonce VARCHAR(64) NOT NULL,
	source_digest VARCHAR(64) NOT NULL,
	supervisor_pid INTEGER,
	supervisor_start_ticks BIGINT,
	supervisor_role VARCHAR(16) DEFAULT 'supervisor' NOT NULL,
	state VARCHAR(16) DEFAULT 'registered' NOT NULL,
	registered_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
	settled_at TIMESTAMP WITH TIME ZONE,
	PRIMARY KEY (attempt_id, pid, start_ticks),
	CONSTRAINT uq_fleet_workspace_process_parent UNIQUE (attempt_id, pid, start_ticks, tool_execution_id, role),
	CONSTRAINT fk_fleet_workspace_process_supervisor FOREIGN KEY(attempt_id, supervisor_pid, supervisor_start_ticks, tool_execution_id, supervisor_role) REFERENCES fleet_workspace_processes (attempt_id, pid,
start_ticks, tool_execution_id, role),
	CONSTRAINT fk_fleet_workspace_process_placement FOREIGN KEY(run_id, agent_task_id, generation, user_id, thread_id) REFERENCES fleet_run_placements (run_id, agent_task_id, generation, user_id, thread_id),
	CONSTRAINT fk_fleet_workspace_process_launch FOREIGN KEY(run_id, agent_task_id, generation, user_id, thread_id, launch_spec_digest) REFERENCES fleet_launch_specs (run_id, agent_task_id, generation, user_id, thread_id, payload_digest),
	CONSTRAINT fk_fleet_workspace_process_attempt FOREIGN KEY(attempt_id, run_id) REFERENCES fleet_attempts (id, run_id),
	CONSTRAINT fk_fleet_workspace_process_execution FOREIGN KEY(attempt_id, run_id, node_id, node_session_id, token_stamp, process_ref) REFERENCES fleet_attempts (id, run_id, node_id, node_session_id, token_hash, process_ref),
	CONSTRAINT ck_fleet_workspace_process_owner CHECK (process_ref = 'fleet-' || attempt_id AND generation > 0 AND owner_worker_id = 'fleet-agent:' || attempt_id),
	CONSTRAINT ck_fleet_workspace_process_digests CHECK (launch_spec_digest ~ '^sha256:[a-f0-9]{64}$' AND token_stamp ~ '^[a-f0-9]{64}$'),
	CONSTRAINT ck_fleet_workspace_process_identity CHECK (pid > 0 AND start_ticks > 0 AND tool_execution_id <> '' AND source_digest ~ '^[a-f0-9]{64}$' AND start_nonce ~ '^[a-f0-9]{64}$'),
	CONSTRAINT ck_fleet_workspace_process_state CHECK (role IN ('supervisor','shell') AND state IN ('registered','settled') AND (state='registered')=(settled_at IS NULL)),
	CONSTRAINT ck_fleet_workspace_process_parent CHECK ((role='supervisor' AND supervisor_pid IS NULL AND supervisor_start_ticks IS NULL) OR (role='shell' AND supervisor_pid IS NOT NULL AND supervisor_start_ticks
IS NOT NULL AND supervisor_pid > 0 AND supervisor_start_ticks > 0 AND supervisor_role='supervisor'))
)
    """,
    """
CREATE INDEX ix_fleet_workspace_process_owner ON fleet_workspace_processes (attempt_id, state)
    """,
)


def upgrade():
    for statement in (*DDL, *PROCESS_DDL):
        op.execute(statement)
    op.execute("""
      CREATE FUNCTION fleet_workspace_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'Fleet workspace evidence is immutable'; END $$
    """)
    for table in ("fleet_workspace_manifests", "fleet_workspace_points"):
        op.execute(f"CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION fleet_workspace_immutable()")
    op.execute("""
      CREATE FUNCTION fleet_workspace_request_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Fleet workspace request evidence is immutable'; END IF;
        IF (to_jsonb(NEW)-ARRAY['state','claim_nonce','claim_lease_expires_at','barrier_epoch','candidate_manifest_id','rejection','updated_at']) IS DISTINCT FROM
           (to_jsonb(OLD)-ARRAY['state','claim_nonce','claim_lease_expires_at','barrier_epoch','candidate_manifest_id','rejection','updated_at']) THEN
           RAISE EXCEPTION 'Fleet workspace request identity is immutable';
        END IF;
        IF OLD.state IN ('accepted','rejected') AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
           RAISE EXCEPTION 'Fleet workspace request outcome is immutable';
        END IF;
        IF NEW.barrier_epoch < OLD.barrier_epoch OR NOT (
            NEW.state=OLD.state OR (OLD.state='requested' AND NEW.state IN ('sealing','rejected')) OR
            (OLD.state='sealing' AND NEW.state IN ('sealing','prepared','rejected')) OR
            (OLD.state='prepared' AND NEW.state IN ('accepted','rejected'))) THEN
           RAISE EXCEPTION 'Invalid workspace request transition';
        END IF;
        RETURN NEW;
      END $$
    """)
    op.execute("""
      CREATE FUNCTION fleet_workspace_process_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP='DELETE' OR (to_jsonb(NEW)-ARRAY['state','settled_at']) IS DISTINCT FROM
           (to_jsonb(OLD)-ARRAY['state','settled_at']) THEN
           RAISE EXCEPTION 'Fleet workspace process identity is immutable';
        END IF;
        IF OLD.state='settled' AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
           RAISE EXCEPTION 'Fleet workspace process settlement is immutable';
        END IF;
        RETURN NEW;
      END $$
    """)
    op.execute("CREATE TRIGGER fleet_workspace_processes_guard BEFORE UPDATE OR DELETE ON fleet_workspace_processes FOR EACH ROW EXECUTE FUNCTION fleet_workspace_process_guard()")
    op.execute("CREATE TRIGGER fleet_workspace_requests_guard BEFORE UPDATE OR DELETE ON fleet_workspace_requests FOR EACH ROW EXECUTE FUNCTION fleet_workspace_request_guard()")
    op.execute("""
      CREATE FUNCTION fleet_workspace_point_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE r fleet_workspace_requests%ROWTYPE;
      BEGIN
        SELECT * INTO r FROM fleet_workspace_requests WHERE id=NEW.request_id;
        IF r.state IS DISTINCT FROM 'prepared' OR r.candidate_manifest_id IS DISTINCT FROM NEW.manifest_id OR
           ROW(NEW.desired_core_status,NEW.desired_task_status,NEW.desired_placement_status,NEW.error,NEW.stop_reason) IS DISTINCT FROM
           ROW(r.desired_core_status,r.desired_task_status,r.desired_placement_status,r.error,r.stop_reason) THEN
            RAISE EXCEPTION 'Workspace point must retain prepared request outcome';
        END IF;
        RETURN NEW;
      END $$
    """)
    op.execute("CREATE TRIGGER fleet_workspace_points_guard BEFORE INSERT ON fleet_workspace_points FOR EACH ROW EXECUTE FUNCTION fleet_workspace_point_guard()")

    op.execute("""
      CREATE FUNCTION fleet_workspace_final_pointer_guard() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.final_workspace_point_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM fleet_workspace_points
            WHERE id=NEW.final_workspace_point_id AND kind IN ('final','paused')
        ) THEN RAISE EXCEPTION 'Final placement pointer requires final or paused workspace point';
        END IF;
        RETURN NEW;
      END $$
    """)
    op.execute("""
      CREATE CONSTRAINT TRIGGER fleet_placement_workspace_final_kind
      AFTER INSERT OR UPDATE ON fleet_run_placements DEFERRABLE INITIALLY DEFERRED
      FOR EACH ROW EXECUTE FUNCTION fleet_workspace_final_pointer_guard()
    """)


def downgrade():
    raise RuntimeError("Fleet workspace downgrade requires drained execution and a backup; immutable evidence cannot be discarded")
