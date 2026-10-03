"""Shared identities and budgets for job and Agent execution attempts."""

from sqlalchemy import JSON, BigInteger, CheckConstraint, Column, DateTime, ForeignKey, ForeignKeyConstraint, Index, Integer, String, Table, Text, UniqueConstraint, text
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
        timestamp("cancel_requested_at", nullable=True),
        timestamp("created_at"),
        timestamp("updated_at"),
        UniqueConstraint("id", "user_id", "thread_id", name="uq_fleet_agent_task_owner"),
        ForeignKeyConstraint(["current_run_id", "id"], ["fleet_run_placements.run_id", "fleet_run_placements.agent_task_id"], name="fk_fleet_agent_task_current_run", deferrable=True, initially="DEFERRED", use_alter=True),
        CheckConstraint("generation > 0 AND continuation_budget >= 0", name="ck_fleet_agent_task_budgets"),
        CheckConstraint("state IN ('queued','running','waiting_jobs','paused','input_required','unknown','succeeded','failed','cancelled','timed_out')", name="ck_fleet_agent_task_state"),
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
        CheckConstraint("state IN ('queued','claimed','running','unknown','succeeded','failed','cancelled','timed_out')", name="ck_fleet_placement_state"),
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
