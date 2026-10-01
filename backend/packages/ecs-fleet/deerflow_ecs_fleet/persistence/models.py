"""Shared identities and budgets for job and Agent execution attempts."""

from sqlalchemy import JSON, CheckConstraint, Column, DateTime, ForeignKey, Index, Integer, String, Table, Text, UniqueConstraint, text
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
