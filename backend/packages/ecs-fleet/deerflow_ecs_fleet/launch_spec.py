"""Immutable JSON launch inputs; references stay private and never contain secrets."""

import hashlib
import json
from datetime import UTC
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_serializer, field_validator, model_validator

from .config import NAME_PATTERN, ExecutionProfile

Identity = Annotated[str, Field(pattern=NAME_PATTERN)]
ModelIdentifier = Annotated[str, Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_./:+-]{0,255}$")]
VersionIdentifier = Annotated[str, Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.+!-]{0,127}$")]
Digest = Annotated[str, Field(pattern=r"^sha256:[a-f0-9]{64}$")]


class FrozenDict(dict):
    def immutable(self, *args, **kwargs):
        raise TypeError("Launch input is immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = __ior__ = immutable


def freeze(value):
    if isinstance(value, dict):
        return FrozenDict({key: freeze(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(freeze(child) for child in value)
    return value


def thaw(value):
    if isinstance(value, dict):
        return {key: thaw(child) for key, child in value.items()}
    if isinstance(value, tuple):
        return [thaw(child) for child in value]
    return value


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SnapshotEntry(FrozenModel):
    name: Identity
    version: VersionIdentifier
    digest: Digest


class Snapshot(FrozenModel):
    entries: tuple[SnapshotEntry, ...]

    @field_validator("entries")
    @classmethod
    def canonical_entries(cls, entries):
        if len({entry.name for entry in entries}) != len(entries):
            raise ValueError("Snapshot names must be unique")
        return tuple(sorted(entries, key=lambda entry: entry.name))


class WorkerCompatibility(FrozenModel):
    runtime_digest: Digest
    skill_snapshot: Snapshot
    plugin_snapshot: Snapshot


class SecretReference(FrozenModel):
    name: Identity
    reference_id: Identity


class AgentResources(FrozenModel):
    cpu_millis: int = Field(strict=True, ge=1, le=1_000_000)
    memory_mib: int = Field(strict=True, ge=1, le=4_194_304)
    agent_units: int = Field(strict=True, ge=1, le=1)


# Execution configuration is trusted normalization, not arbitrary operator
# model/database settings. These values must instead be delivered by secret_refs.
SECRET_KEYS = frozenset(
    {
        "secret",
        "secrets",
        "api_key",
        "password",
        "password_hash",
        "token",
        "access_token",
        "authorization",
        "database_url",
        "postgres_url",
        "connection_string",
        "credential",
        "credentials",
        "__active_skill_secrets",
        "__slash_skill_secret_source",
    }
)


def check_configuration(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower().replace("-", "_") in SECRET_KEYS:
                raise ValueError("Execution secrets require out-of-band references")
            check_configuration(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            check_configuration(child)
    elif isinstance(value, str) and (("://" in value and "@" in value) or value.startswith(("Bearer ", "df_fleet_", "dfp_", "sk-"))):
        raise ValueError("Execution secrets require out-of-band references")


class LaunchSpec(FrozenModel):
    schema_version: int = Field(strict=True, ge=1, le=1)
    run_id: Identity
    agent_task_id: Identity
    generation: int = Field(strict=True, ge=1)
    user_id: Identity
    thread_id: Identity
    assistant_id: Identity
    profile: Identity
    model_name: ModelIdentifier
    model_version: VersionIdentifier
    input: JsonValue
    normalized_config: dict[str, JsonValue]
    stream_modes: tuple[Literal["values", "messages-tuple", "updates", "debug", "tasks", "checkpoints", "custom"], ...] = Field(min_length=1)
    stream_subgraphs: bool = Field(strict=True)
    interrupt_before: tuple[Identity, ...] | Literal["*"] | None
    interrupt_after: tuple[Identity, ...] | Literal["*"] | None
    recursion_limit: int = Field(strict=True, ge=1)
    execution_deadline: AwareDatetime
    runtime_digest: Digest
    skill_snapshot: Snapshot
    plugin_snapshot: Snapshot
    workspace_manifest_ref: Identity
    secret_refs: tuple[SecretReference, ...]
    resources: AgentResources

    @field_validator("execution_deadline")
    @classmethod
    def utc_deadline(cls, value):
        if value.utcoffset().total_seconds() != 0:
            raise ValueError("Execution deadline must be UTC")
        return value.astimezone(UTC)

    @field_validator("input", "normalized_config")
    @classmethod
    def immutable_json(cls, value):
        # Reject nonfinite numbers as well as copying before recursively freezing.
        json.dumps(value, allow_nan=False)
        return freeze(value)

    @field_serializer("input", "normalized_config")
    def json_values(self, value):
        return thaw(value)

    @model_validator(mode="after")
    def complete_execution(self):
        check_configuration(self.normalized_config)
        if self.normalized_config.get("recursion_limit") != self.recursion_limit:
            raise ValueError("Normalized recursion budget must match the launch")
        configurable = self.normalized_config.get("configurable")
        if not isinstance(configurable, dict) or configurable.get("thread_id") != self.thread_id:
            raise ValueError("Normalized checkpoint thread identity required")
        for section in ("configurable", "context"):
            context = self.normalized_config.get(section, {})
            if not isinstance(context, dict):
                raise ValueError("Normalized execution context must be an object")
            if "user_id" in context and context["user_id"] != self.user_id:
                raise ValueError("Normalized user identity mismatch")
            if "thread_id" in context and context["thread_id"] != self.thread_id:
                raise ValueError("Normalized thread identity mismatch")
        if len({ref.name for ref in self.secret_refs}) != len(self.secret_refs):
            raise ValueError("Secret binding names must be unique")
        return self

    def canonical_payload(self):
        return self.model_dump(mode="json")

    def payload_digest(self):
        payload = json.dumps(self.canonical_payload(), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        return "sha256:" + hashlib.sha256(payload.encode()).hexdigest()

    def public_summary(self):
        # No input/config, references, snapshot entries or private launch payload.
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "agent_task_id": self.agent_task_id,
            "generation": self.generation,
            "profile": self.profile,
            "recursion_limit": self.recursion_limit,
            "execution_deadline": self.execution_deadline.isoformat(),
        }


def build_launch_spec(*, profile_name: str, profile: ExecutionProfile, run_parameters: dict):
    if profile.kind != "agent" or profile.runtime_digest is None:
        raise ValueError("Remote launch requires an approved agent profile")
    if {"profile", "runtime_digest", "resources"} & run_parameters.keys():
        raise ValueError("Operator launch fields cannot be supplied by run parameters")
    return LaunchSpec.model_validate(run_parameters | {"profile": profile_name, "runtime_digest": profile.runtime_digest, "resources": {"cpu_millis": profile.cpu_millis, "memory_mib": profile.memory_mib, "agent_units": 1}})
