"""Shared identities and budgets for job and Agent execution attempts."""

from sqlalchemy import JSON, BigInteger, CheckConstraint, Column, DateTime, ForeignKey, ForeignKeyConstraint, Index, Integer, PrimaryKeyConstraint, String, Table, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB

from .base import FleetBase

metadata = FleetBase.metadata
json_type = JSON().with_variant(JSONB(), "postgresql")
JOB_ACTIVE = "state IN ('staged','queued','claimed','running','unknown','quarantined')"
ATTEMPT_ACTIVE = "state IN ('claimed','starting','running','unknown','quarantined')"


def timestamp(name, *, nullable=False):
    return Column(name, DateTime(timezone=True), nullable=nullable, server_default=None if nullable else text("clock_timestamp()"))


class NodeRow(FleetBase):
    __table__ = Table(
        "fleet_nodes",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("name", String(128), nullable=False, unique=True),
        Column("profile_allowlist", json_type),
        Column("registered_by", String(64)),
        Column("admin_state", String(16), nullable=False, server_default="enabled"),
        Column("health", String(16), nullable=False, server_default="unknown"),
        Column("session_id", String(64)),
        Column("protocol_version", Integer, nullable=False, server_default="1"),
        Column("runtime_digest", String(128)),
        Column("cpu_millis", Integer, nullable=False),
        Column("memory_mib", Integer, nullable=False),
        Column("agent_limit", Integer, nullable=False, server_default="0"),
        timestamp("last_seen_at", nullable=True),
        timestamp("created_at"),
        timestamp("updated_at"),
        CheckConstraint("cpu_millis > 0 AND memory_mib > 0 AND agent_limit >= 0", name="ck_fleet_nodes_capacity"),
        CheckConstraint("profile_allowlist IS NULL OR jsonb_typeof(profile_allowlist) = 'array'", name="ck_fleet_nodes_profiles"),
        CheckConstraint("admin_state IN ('enabled','draining','disabled')", name="ck_fleet_nodes_admin"),
        CheckConstraint("health IN ('online','offline','unknown')", name="ck_fleet_nodes_health"),
    )


class JobRow(FleetBase):
    __table__ = Table(
        "fleet_jobs",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("user_id", String(64), nullable=False),
        Column("thread_id", String(64), nullable=False),
        Column("source_run_id", String(64)),
        Column("tracking_task_id", String(64), nullable=False),
        Column("idempotency_key", String(128), nullable=False),
        Column("dedupe_group", String(128)),
        Column("spec", json_type, nullable=False),
        Column("state", String(24), nullable=False, server_default="staged"),
        Column("active_attempt_id", String(64)),
        Column("accepted_manifest_id", String(64)),
        Column("error", Text),
        timestamp("staged_deadline"),
        timestamp("queue_deadline"),
        timestamp("queued_at", nullable=True),
        timestamp("cancel_requested_at", nullable=True),
        timestamp("finished_at", nullable=True),
        timestamp("created_at"),
        timestamp("updated_at"),
        UniqueConstraint("user_id", "idempotency_key", name="uq_fleet_jobs_submission"),
        UniqueConstraint("user_id", "tracking_task_id", name="uq_fleet_jobs_tracking"),
        CheckConstraint("state IN ('staged','queued','claimed','running','succeeded','failed','cancelled','unknown','quarantined')", name="ck_fleet_jobs_state"),
        Index("uq_fleet_jobs_active_group", "user_id", "dedupe_group", unique=True, postgresql_where=text("dedupe_group IS NOT NULL AND " + JOB_ACTIVE)),
        Index("ix_fleet_jobs_due", "state", "queue_deadline"),
        Index("ix_fleet_jobs_thread", "user_id", "thread_id", "created_at"),
    )


class AttemptRow(FleetBase):
    __table__ = Table(
        "fleet_attempts",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("kind", String(16), nullable=False),
        Column("job_id", String(64), ForeignKey("fleet_jobs.id")),
        Column("run_id", String(64)),
        Column("attempt_no", Integer, nullable=False),
        Column("node_id", String(64), ForeignKey("fleet_nodes.id"), nullable=False),
        Column("node_session_id", String(64), nullable=False),
        Column("token_hash", String(64), nullable=False),
        Column("state", String(24), nullable=False, server_default="claimed"),
        Column("process_ref", String(128)),
        Column("output_prefix", String(512), nullable=False),
        Column("outcome", json_type),
        Column("launch_spec", json_type),
        timestamp("lease_expires_at"),
        timestamp("execution_deadline", nullable=True),
        timestamp("start_authorized_at", nullable=True),
        timestamp("started_at", nullable=True),
        timestamp("stopped_at", nullable=True),
        timestamp("finished_at", nullable=True),
        timestamp("created_at"),
        CheckConstraint("(kind='job' AND job_id IS NOT NULL AND run_id IS NULL) OR (kind='agent' AND run_id IS NOT NULL AND job_id IS NULL)", name="ck_fleet_attempt_identity"),
        CheckConstraint("attempt_no > 0", name="ck_fleet_attempt_number"),
        CheckConstraint("state IN ('claimed','starting','running','succeeded','failed','cancelled','expired','unknown','quarantined')", name="ck_fleet_attempt_state"),
        UniqueConstraint("job_id", "attempt_no", name="uq_fleet_attempt_job_number"),
        UniqueConstraint("run_id", "attempt_no", name="uq_fleet_attempt_run_number"),
        Index("uq_fleet_attempt_job_active", "job_id", unique=True, postgresql_where=text("job_id IS NOT NULL AND " + ATTEMPT_ACTIVE)),
        Index("uq_fleet_attempt_run_active", "run_id", unique=True, postgresql_where=text("run_id IS NOT NULL AND " + ATTEMPT_ACTIVE)),
    )


class ReservationRow(FleetBase):
    __table__ = Table(
        "fleet_reservations",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("node_id", String(64), ForeignKey("fleet_nodes.id"), nullable=False),
        Column("attempt_id", String(64), ForeignKey("fleet_attempts.id"), nullable=False, unique=True),
        Column("cpu_millis", Integer, nullable=False),
        Column("memory_mib", Integer, nullable=False),
        Column("agent_units", Integer, nullable=False, server_default="0"),
        Column("state", String(24), nullable=False, server_default="reserved"),
        timestamp("created_at"),
        timestamp("released_at", nullable=True),
        CheckConstraint("cpu_millis > 0 AND memory_mib > 0 AND agent_units >= 0", name="ck_fleet_reservation_capacity"),
        CheckConstraint("state IN ('reserved','active','quarantined','released')", name="ck_fleet_reservation_state"),
        CheckConstraint("(state='released') = (released_at IS NOT NULL)", name="ck_fleet_reservation_release"),
        Index("ix_fleet_reservations_node", "node_id", "state"),
    )


class CredentialRow(FleetBase):
    __table__ = Table(
        "fleet_credentials",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("node_id", String(64), ForeignKey("fleet_nodes.id"), nullable=False),
        Column("token_hash", String(64), nullable=False, unique=True),
        timestamp("expires_at"),
        timestamp("revoked_at", nullable=True),
        timestamp("created_at"),
    )


class ArtifactManifestRow(FleetBase):
    __table__ = Table(
        "fleet_artifact_manifests",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("attempt_id", String(64), ForeignKey("fleet_attempts.id"), nullable=False, unique=True),
        Column("user_id", String(64), nullable=False),
        Column("thread_id", String(64), nullable=False),
        Column("output_prefix", String(512), nullable=False),
        Column("files", json_type, nullable=False),
        Column("total_bytes", Integer, nullable=False),
        timestamp("sealed_at"),
        CheckConstraint("total_bytes >= 0", name="ck_fleet_manifest_bytes"),
    )


class InputManifestRow(FleetBase):
    __table__ = Table(
        "fleet_input_manifests",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("user_id", String(64), nullable=False),
        Column("thread_id", String(64), nullable=False),
        Column("output_prefix", String(512), nullable=False, unique=True),
        Column("files", json_type, nullable=False),
        Column("total_bytes", Integer, nullable=False),
        timestamp("sealed_at"),
        CheckConstraint("total_bytes >= 0", name="ck_fleet_input_bytes"),
        Index("ix_fleet_inputs_owner", "user_id", "thread_id", "sealed_at"),
    )


class RecoveryEventRow(FleetBase):
    __table__ = Table(
        "fleet_recovery_events",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("job_id", String(64), ForeignKey("fleet_jobs.id"), nullable=False),
        Column("attempt_id", String(64), ForeignKey("fleet_attempts.id"), nullable=False, unique=True),
        Column("operator_id", String(64), nullable=False),
        Column("action", String(32), nullable=False),
        Column("note", Text, nullable=False),
        timestamp("created_at"),
        CheckConstraint("action = 'fail_stopped'", name="ck_fleet_recovery_action"),
        Index("ix_fleet_recovery_job", "job_id", "created_at"),
    )


class JobInvocationRow(FleetBase):
    """Immutable admission receipt, including invocations that reused work."""

    __table__ = Table(
        "fleet_job_invocations",
        metadata,
        Column("user_id", String(64), primary_key=True),
        Column("idempotency_key", String(128), primary_key=True),
        Column("thread_id", String(64), nullable=False),
        Column("source_run_id", String(64)),
        Column("spec", json_type, nullable=False),
        Column("dedupe_group", String(128)),
        Column("job_id", String(64), ForeignKey("fleet_jobs.id"), nullable=False),
        timestamp("created_at"),
        Index("ix_fleet_job_invocations_job", "job_id"),
    )


AGENT_TASK_ACTIVE = "state NOT IN ('succeeded','failed','cancelled','timed_out')"


class AgentTaskRow(FleetBase):
    __table__ = Table(
        "fleet_agent_tasks",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("user_id", String(64), nullable=False),
        Column("thread_id", String(64), nullable=False),
        Column("state", String(24), nullable=False, server_default="queued"),
        Column("current_run_id", String(64)),
        Column("generation", Integer, nullable=False, server_default="1"),
        timestamp("deadline"),
        Column("continuation_budget", Integer, nullable=False),
        Column("wait_group_id", String(64)),
        Column("accepted_workspace_point_id", String(64)),
        timestamp("cancel_requested_at", nullable=True),
        timestamp("created_at"),
        timestamp("updated_at"),
        UniqueConstraint("id", "user_id", "thread_id", name="uq_fleet_agent_task_owner"),
        ForeignKeyConstraint(["current_run_id", "id"], ["fleet_run_placements.run_id", "fleet_run_placements.agent_task_id"], name="fk_fleet_agent_task_current_run", deferrable=True, initially="DEFERRED", use_alter=True),
        CheckConstraint("generation > 0 AND continuation_budget >= 0", name="ck_fleet_agent_task_budgets"),
        CheckConstraint("state IN ('queued','running','waiting_jobs','paused','input_required','unknown','finishing','recovery_required','succeeded','failed','cancelled','timed_out')", name="ck_fleet_agent_task_state"),
        Index("uq_fleet_agent_task_active_thread", "user_id", "thread_id", unique=True, postgresql_where=text(AGENT_TASK_ACTIVE)),
    )


class LaunchSpecRow(FleetBase):
    __table__ = Table(
        "fleet_launch_specs",
        metadata,
        Column("id", String(64), primary_key=True),
        Column("run_id", String(64), nullable=False, unique=True),
        Column("agent_task_id", String(64), nullable=False),
        Column("generation", Integer, nullable=False),
        Column("user_id", String(64), nullable=False),
        Column("thread_id", String(64), nullable=False),
        Column("payload", json_type, nullable=False),
        Column("payload_digest", String(71), nullable=False),
        timestamp("created_at"),
        ForeignKeyConstraint(["agent_task_id", "user_id", "thread_id"], ["fleet_agent_tasks.id", "fleet_agent_tasks.user_id", "fleet_agent_tasks.thread_id"], name="fk_fleet_launch_spec_owner"),
        UniqueConstraint("id", "run_id", "agent_task_id", "generation", "user_id", "thread_id", name="uq_fleet_launch_spec_identity"),
        CheckConstraint("generation > 0", name="ck_fleet_launch_spec_generation"),
        CheckConstraint("payload_digest ~ '^sha256:[a-f0-9]{64}$'", name="ck_fleet_launch_spec_digest"),
    )


class RunPlacementRow(FleetBase):
    __table__ = Table(
        "fleet_run_placements",
        metadata,
        Column("run_id", String(64), primary_key=True),
        Column("agent_task_id", String(64), nullable=False),
        Column("generation", Integer, nullable=False),
        Column("user_id", String(64), nullable=False),
        Column("thread_id", String(64), nullable=False),
        Column("requested_backend", String(16), nullable=False),
        Column("node_id", String(64), ForeignKey("fleet_nodes.id")),
        Column("profile", String(64), nullable=False),
        Column("state", String(24), nullable=False, server_default="queued"),
        Column("active_attempt_id", String(64), ForeignKey("fleet_attempts.id")),
        Column("launch_spec_ref", String(64), nullable=False, unique=True),
        Column("final_workspace_point_id", String(64)),
        timestamp("queue_deadline"),
        timestamp("created_at"),
        timestamp("updated_at"),
        UniqueConstraint("run_id", "agent_task_id", name="uq_fleet_run_placement_task"),
        ForeignKeyConstraint(
            ["launch_spec_ref", "run_id", "agent_task_id", "generation", "user_id", "thread_id"],
            ["fleet_launch_specs.id", "fleet_launch_specs.run_id", "fleet_launch_specs.agent_task_id", "fleet_launch_specs.generation", "fleet_launch_specs.user_id", "fleet_launch_specs.thread_id"],
            name="fk_fleet_placement_launch_identity",
        ),
        CheckConstraint("generation > 0", name="ck_fleet_placement_generation"),
        CheckConstraint("requested_backend IN ('remote','auto')", name="ck_fleet_placement_backend"),
        CheckConstraint("state IN ('queued','claimed','running','unknown','finishing','recovery_required','succeeded','failed','cancelled','timed_out')", name="ck_fleet_placement_state"),
    )


class EventOutboxRow(FleetBase):
    __table__ = Table(
        "fleet_event_outbox",
        metadata,
        Column("run_id", String(64), ForeignKey("fleet_run_placements.run_id"), primary_key=True),
        Column("attempt_id", String(64), ForeignKey("fleet_attempts.id"), primary_key=True),
        Column("seq", BigInteger, primary_key=True),
        Column("generation", Integer, nullable=False),
        Column("user_id", String(64), nullable=False),
        Column("thread_id", String(64), nullable=False),
        Column("launch_spec_digest", String(71), nullable=False),
        Column("event_id", BigInteger, nullable=False, unique=True),
        timestamp("published_at", nullable=True),
        timestamp("created_at"),
        UniqueConstraint("thread_id", "seq", name="uq_fleet_event_outbox_thread_seq"),
        CheckConstraint("generation > 0 AND seq > 0", name="ck_fleet_event_outbox_sequence"),
        Index("ix_fleet_event_outbox_pending", "run_id", "attempt_id", "seq", postgresql_where=text("published_at IS NULL")),
    )


class StreamSealRow(FleetBase):
    __table__ = Table(
        "fleet_stream_seals",
        metadata,
        Column("run_id", String(64), ForeignKey("fleet_run_placements.run_id"), primary_key=True),
        Column("attempt_id", String(64), ForeignKey("fleet_attempts.id"), primary_key=True),
        Column("generation", Integer, nullable=False),
        Column("user_id", String(64), nullable=False),
        Column("thread_id", String(64), nullable=False),
        Column("launch_spec_digest", String(71), nullable=False),
        Column("last_seq", BigInteger, nullable=False),
        Column("core_status", String(16), nullable=False),
        Column("source", String(16), nullable=False),
        timestamp("created_at"),
        CheckConstraint("generation > 0 AND last_seq >= 0", name="ck_fleet_stream_seal_sequence"),
        CheckConstraint("core_status IN ('success','error','interrupted','timeout')", name="ck_fleet_stream_seal_status"),
        CheckConstraint("source IN ('writer','physical_stop')", name="ck_fleet_stream_seal_source"),
    )


# C ownership keys bind the immutable execution chain inside Fleet metadata.
_WORKSPACE_OWNER = ("run_id", "agent_task_id", "generation", "user_id", "thread_id")
_WORKSPACE_EXECUTION = (*_WORKSPACE_OWNER, "attempt_id", "launch_spec_digest", "node_id", "node_session_id", "token_stamp", "process_ref")
AttemptRow.__table__.append_constraint(UniqueConstraint("id", "run_id", name="uq_fleet_attempt_run_identity"))
AttemptRow.__table__.append_constraint(UniqueConstraint("id", "run_id", "node_id", "node_session_id", "token_hash", "process_ref", name="uq_fleet_attempt_original_execution"))
RunPlacementRow.__table__.append_constraint(UniqueConstraint(*_WORKSPACE_OWNER, name="uq_fleet_placement_workspace_owner"))
LaunchSpecRow.__table__.append_constraint(UniqueConstraint(*_WORKSPACE_OWNER, "payload_digest", name="uq_fleet_launch_workspace_digest"))


def workspace_identity_columns():
    return [Column(name, Integer if name == "generation" else String(128 if name in {"process_ref", "owner_worker_id"} else 71 if name == "launch_spec_digest" else 64), nullable=False) for name in (*_WORKSPACE_EXECUTION, "owner_worker_id")]


def workspace_identity_constraints(prefix):
    return [
        ForeignKeyConstraint(list(_WORKSPACE_OWNER), ["fleet_run_placements." + name for name in _WORKSPACE_OWNER], name="fk_" + prefix + "_placement"),
        ForeignKeyConstraint([*_WORKSPACE_OWNER, "launch_spec_digest"], [*("fleet_launch_specs." + name for name in _WORKSPACE_OWNER), "fleet_launch_specs.payload_digest"], name="fk_" + prefix + "_launch"),
        ForeignKeyConstraint(["attempt_id", "run_id"], ["fleet_attempts.id", "fleet_attempts.run_id"], name="fk_" + prefix + "_attempt"),
        ForeignKeyConstraint(
            ["attempt_id", "run_id", "node_id", "node_session_id", "token_stamp", "process_ref"],
            ["fleet_attempts.id", "fleet_attempts.run_id", "fleet_attempts.node_id", "fleet_attempts.node_session_id", "fleet_attempts.token_hash", "fleet_attempts.process_ref"],
            name="fk_" + prefix + "_execution",
        ),
        CheckConstraint("process_ref = 'fleet-' || attempt_id AND generation > 0 AND owner_worker_id = 'fleet-agent:' || attempt_id", name="ck_" + prefix + "_owner"),
        CheckConstraint("launch_spec_digest ~ '^sha256:[a-f0-9]{64}$' AND token_stamp ~ '^[a-f0-9]{64}$'", name="ck_" + prefix + "_digests"),
    ]


class WorkspaceRequestRow(FleetBase):
    __table__ = Table(
        "fleet_workspace_requests",
        metadata,
        Column("id", String(64), primary_key=True),
        *workspace_identity_columns(),
        Column("request_digest", String(64), nullable=False),
        Column("checkpoint_ns", String(64), nullable=False, server_default=""),
        Column("checkpoint_id", String(128), nullable=False),
        Column("kind", String(16), nullable=False),
        Column("publication_key", String(128), nullable=False),
        Column("presented_paths", json_type, nullable=False),
        Column("source_workspace_version", String(128), nullable=False),
        Column("desired_core_status", String(16)),
        Column("desired_task_status", String(24)),
        Column("desired_placement_status", String(24)),
        Column("error", Text),
        Column("stop_reason", String(128)),
        Column("state", String(16), nullable=False, server_default="requested"),
        Column("claim_nonce", String(64)),
        timestamp("claim_lease_expires_at", nullable=True),
        Column("barrier_epoch", BigInteger, nullable=False, server_default="0"),
        Column("candidate_manifest_id", String(64)),
        Column("rejection", Text),
        timestamp("created_at"),
        timestamp("updated_at"),
        *workspace_identity_constraints("fleet_workspace_request"),
        UniqueConstraint("attempt_id", "checkpoint_id", "kind", "publication_key", name="uq_fleet_workspace_request_publication"),
        UniqueConstraint("id", *_WORKSPACE_EXECUTION, "owner_worker_id", "request_digest", "checkpoint_ns", "checkpoint_id", "kind", "publication_key", name="uq_fleet_workspace_request_identity"),
        CheckConstraint("checkpoint_ns = '' AND checkpoint_id <> '' AND publication_key <> '' AND source_workspace_version <> ''", name="ck_fleet_workspace_request_boundary"),
        CheckConstraint("kind IN ('partial','final','paused') AND state IN ('requested','sealing','prepared','accepted','rejected')", name="ck_fleet_workspace_request_state"),
        CheckConstraint("request_digest ~ '^[a-f0-9]{64}$' AND jsonb_typeof(presented_paths) = 'array'", name="ck_fleet_workspace_request_content"),
        CheckConstraint("barrier_epoch >= 0 AND (claim_nonce IS NULL) = (claim_lease_expires_at IS NULL)", name="ck_fleet_workspace_request_claim"),
        CheckConstraint(
            """
(kind='partial' AND desired_core_status IS NULL AND desired_task_status IS NULL AND desired_placement_status IS NULL AND error IS NULL AND stop_reason IS NULL) OR (kind IN
('final','paused') AND desired_core_status IS NOT NULL AND desired_task_status IS NOT NULL AND desired_placement_status IS NOT NULL AND desired_core_status IN
('success','error','interrupted','timeout') AND desired_task_status IN ('succeeded','failed','cancelled','timed_out','paused','input_required') AND desired_placement_status IN
('succeeded','failed','cancelled','timed_out'))
""",
            name="ck_fleet_workspace_request_outcome",
        ),
        Index("ix_fleet_workspace_request_pending", "state", "claim_lease_expires_at"),
    )


class WorkspaceManifestRow(FleetBase):
    __table__ = Table(
        "fleet_workspace_manifests",
        metadata,
        Column("id", String(64), primary_key=True),
        *workspace_identity_columns(),
        Column("request_digest", String(64), nullable=False),
        Column("content_hash", String(64), nullable=False),
        Column("schema_version", Integer, nullable=False, server_default="1"),
        Column("categories", json_type, nullable=False),
        Column("directories", json_type, nullable=False),
        Column("files", json_type, nullable=False),
        Column("total_bytes", BigInteger, nullable=False),
        Column("nas_prefix", String(512), nullable=False, unique=True),
        timestamp("sealed_at"),
        *workspace_identity_constraints("fleet_workspace_manifest"),
        UniqueConstraint("id", *_WORKSPACE_EXECUTION, "owner_worker_id", "request_digest", name="uq_fleet_workspace_manifest_identity"),
        CheckConstraint("schema_version=1 AND total_bytes>=0 AND id=content_hash AND content_hash ~ '^[a-f0-9]{64}$' AND request_digest ~ '^[a-f0-9]{64}$'", name="ck_fleet_workspace_manifest_content"),
        CheckConstraint("categories = '[\"workspace\",\"uploads\",\"outputs\"]'::jsonb AND jsonb_typeof(files)='array' AND jsonb_typeof(directories)='array'", name="ck_fleet_workspace_manifest_inventory"),
    )


class WorkspacePointRow(FleetBase):
    __table__ = Table(
        "fleet_workspace_points",
        metadata,
        Column("id", String(64), primary_key=True),
        *workspace_identity_columns(),
        Column("request_id", String(64), nullable=False, unique=True),
        Column("request_digest", String(64), nullable=False),
        Column("checkpoint_ns", String(64), nullable=False, server_default=""),
        Column("checkpoint_id", String(128), nullable=False),
        Column("manifest_id", String(64), nullable=False),
        Column("kind", String(16), nullable=False),
        Column("publication_key", String(128), nullable=False),
        Column("desired_core_status", String(16)),
        Column("desired_task_status", String(24)),
        Column("desired_placement_status", String(24)),
        Column("error", Text),
        Column("stop_reason", String(128)),
        timestamp("accepted_at"),
        *workspace_identity_constraints("fleet_workspace_point"),
        UniqueConstraint("id", "agent_task_id", "user_id", "thread_id", name="uq_fleet_workspace_point_task_owner"),
        UniqueConstraint("id", *_WORKSPACE_OWNER, name="uq_fleet_workspace_point_run_owner"),
        ForeignKeyConstraint(
            ["request_id", *_WORKSPACE_EXECUTION, "owner_worker_id", "request_digest", "checkpoint_ns", "checkpoint_id", "kind", "publication_key"],
            ["fleet_workspace_requests." + name for name in ("id", *_WORKSPACE_EXECUTION, "owner_worker_id", "request_digest", "checkpoint_ns", "checkpoint_id", "kind", "publication_key")],
            name="fk_fleet_workspace_point_request",
        ),
        ForeignKeyConstraint(
            ["manifest_id", *_WORKSPACE_EXECUTION, "owner_worker_id", "request_digest"],
            ["fleet_workspace_manifests." + name for name in ("id", *_WORKSPACE_EXECUTION, "owner_worker_id", "request_digest")],
            name="fk_fleet_workspace_point_manifest",
        ),
        CheckConstraint("checkpoint_ns='' AND kind IN ('partial','final','paused')", name="ck_fleet_workspace_point_boundary"),
        CheckConstraint(
            """
(kind='partial' AND desired_core_status IS NULL AND desired_task_status IS NULL AND desired_placement_status IS NULL AND error IS NULL AND stop_reason IS NULL) OR (kind IN
('final','paused') AND desired_core_status IS NOT NULL AND desired_task_status IS NOT NULL AND desired_placement_status IS NOT NULL AND desired_core_status IN
('success','error','interrupted','timeout') AND desired_task_status IN ('succeeded','failed','cancelled','timed_out','paused','input_required') AND desired_placement_status IN
('succeeded','failed','cancelled','timed_out'))
""",
            name="ck_fleet_workspace_point_outcome",
        ),
        Index("uq_fleet_workspace_point_final_run", "run_id", unique=True, postgresql_where=text("kind IN ('final','paused')")),
    )


AgentTaskRow.__table__.append_constraint(
    ForeignKeyConstraint(
        ["accepted_workspace_point_id", "id", "user_id", "thread_id"],
        ["fleet_workspace_points.id", "fleet_workspace_points.agent_task_id", "fleet_workspace_points.user_id", "fleet_workspace_points.thread_id"],
        name="fk_fleet_agent_task_workspace_point",
        deferrable=True,
        initially="DEFERRED",
        use_alter=True,
    )
)
RunPlacementRow.__table__.append_constraint(
    ForeignKeyConstraint(
        ["final_workspace_point_id", *_WORKSPACE_OWNER],
        ["fleet_workspace_points.id", *("fleet_workspace_points." + name for name in _WORKSPACE_OWNER)],
        name="fk_fleet_placement_workspace_point",
        deferrable=True,
        initially="DEFERRED",
        use_alter=True,
    )
)
WorkspaceRequestRow.__table__.append_constraint(
    ForeignKeyConstraint(
        ["candidate_manifest_id", *_WORKSPACE_EXECUTION, "owner_worker_id", "request_digest"],
        ["fleet_workspace_manifests." + name for name in ("id", *_WORKSPACE_EXECUTION, "owner_worker_id", "request_digest")],
        name="fk_fleet_workspace_request_candidate",
        deferrable=True,
        initially="DEFERRED",
        use_alter=True,
    )
)

WorkspaceRequestRow.__table__.append_constraint(UniqueConstraint("request_digest", *_WORKSPACE_EXECUTION, "owner_worker_id", name="uq_fleet_workspace_request_manifest_owner"))
WorkspaceManifestRow.__table__.append_constraint(
    ForeignKeyConstraint(
        ["request_digest", *_WORKSPACE_EXECUTION, "owner_worker_id"], ["fleet_workspace_requests." + name for name in ("request_digest", *_WORKSPACE_EXECUTION, "owner_worker_id")], name="fk_fleet_workspace_manifest_request"
    )
)


class WorkspaceProcessRow(FleetBase):
    __table__ = Table(
        "fleet_workspace_processes",
        metadata,
        *workspace_identity_columns(),
        Column("pid", Integer, nullable=False),
        Column("start_ticks", BigInteger, nullable=False),
        PrimaryKeyConstraint("attempt_id", "pid", "start_ticks"),
        Column("role", String(16), nullable=False),
        Column("tool_execution_id", String(64), nullable=False),
        Column("start_nonce", String(64), nullable=False),
        Column("source_digest", String(64), nullable=False),
        Column("supervisor_pid", Integer),
        Column("supervisor_start_ticks", BigInteger),
        Column("supervisor_role", String(16), nullable=False, server_default="supervisor"),
        UniqueConstraint("attempt_id", "pid", "start_ticks", "tool_execution_id", "role", name="uq_fleet_workspace_process_parent"),
        ForeignKeyConstraint(
            ["attempt_id", "supervisor_pid", "supervisor_start_ticks", "tool_execution_id", "supervisor_role"],
            ["fleet_workspace_processes." + name for name in ("attempt_id", "pid", "start_ticks", "tool_execution_id", "role")],
            name="fk_fleet_workspace_process_supervisor",
        ),
        Column("state", String(16), nullable=False, server_default="registered"),
        timestamp("registered_at"),
        timestamp("settled_at", nullable=True),
        *workspace_identity_constraints("fleet_workspace_process"),
        CheckConstraint("pid > 0 AND start_ticks > 0 AND tool_execution_id <> '' AND source_digest ~ '^[a-f0-9]{64}$' AND start_nonce ~ '^[a-f0-9]{64}$'", name="ck_fleet_workspace_process_identity"),
        CheckConstraint("role IN ('supervisor','shell') AND state IN ('registered','settled') AND (state='registered')=(settled_at IS NULL)", name="ck_fleet_workspace_process_state"),
        CheckConstraint(
            "(role='supervisor' AND supervisor_pid IS NULL AND supervisor_start_ticks IS NULL) OR "
            "(role='shell' AND supervisor_pid IS NOT NULL AND supervisor_start_ticks IS NOT NULL "
            "AND supervisor_pid > 0 AND supervisor_start_ticks > 0 AND supervisor_role='supervisor')",
            name="ck_fleet_workspace_process_parent",
        ),
        Index("ix_fleet_workspace_process_owner", "attempt_id", "state"),
    )
