"""Operator-owned execution budgets; no worker can enlarge these settings."""

from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

IMAGE_PATTERN = r"^(?:[^\s]+@)?sha256:[a-f0-9]{64}$"
NAME_PATTERN = r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$"


class ExecutionProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["job", "agent"] = "job"
    runtime_digest: str | None = Field(default=None, pattern=r"^sha256:[a-f0-9]{64}$")
    image: str = Field(pattern=IMAGE_PATTERN)
    cpu_millis: int = Field(gt=0)
    memory_mib: int = Field(gt=0)
    execution_timeout_seconds: int = Field(default=1800, gt=0)
    network: Literal["none", "bridge"] = "none"
    max_output_bytes: int = Field(default=64 * 1024 * 1024, gt=0)
    max_log_bytes: int = Field(default=4 * 1024 * 1024, gt=0)
    pids_limit: int = Field(default=128, gt=0)
    user: str = "65534:65534"

    @model_validator(mode="after")
    def pinned_agent_runtime(self):
        if self.kind == "agent" and self.runtime_digest is None:
            raise ValueError("Agent profiles require a pinned runtime digest")
        if self.kind == "job" and self.runtime_digest is not None:
            raise ValueError("Runtime digest is an agent-only profile setting")
        return self

    def job_wire(self):
        if self.kind != "job":
            raise ValueError("Only job profiles use the B v1 worker protocol")
        return self.model_dump(exclude={"runtime_digest"})

    @field_validator("user")
    @classmethod
    def non_root_user(cls, value: str) -> str:
        uid, _, gid = value.partition(":")
        if not uid.isdecimal() or int(uid) == 0 or (gid and not gid.isdecimal()):
            raise ValueError("Execution profiles must use an explicit non-root numeric UID")
        return value


class AgentRoutingBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    allowed_user_ids: tuple[str, ...] = Field(min_length=1)
    model_name: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    compatibility: dict
    secret_refs: tuple[dict, ...] = ()
    continuation_budget: int = Field(default=0, ge=0)

    @field_validator("compatibility")
    @classmethod
    def actual_installed_contract(cls, value):
        from .launch_spec import WorkerCompatibility

        return WorkerCompatibility.model_validate(value).model_dump(mode="json")


class FleetConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = False
    jobs_enabled: bool = False
    agents_enabled: bool = False
    continuations_enabled: bool = False
    lease_seconds: int = Field(default=120, ge=30)
    renew_seconds: int = Field(default=30, ge=1)
    queue_timeout_seconds: int = Field(default=1800, ge=1)
    staged_timeout_seconds: int = Field(default=600, ge=1)
    nas_root: Path | None = None
    nas_identity: str | None = Field(default=None, pattern=NAME_PATTERN)
    max_input_bytes: int = Field(default=64 * 1024 * 1024, gt=0, le=2**31 - 1)
    profiles: dict[str, ExecutionProfile] = Field(default_factory=dict)
    scheduled_job_slots: dict[str, str] = Field(default_factory=dict)
    agent_bindings: dict[str, AgentRoutingBinding] = Field(default_factory=dict)
    ticket_seconds: int = Field(default=60, ge=1, le=120)
    scheduling_mode: Literal["reserved", "serial"] = "reserved"
    reserved_job_profile: str | None = None

    @field_validator("profiles")
    @classmethod
    def valid_profile_names(cls, value: dict[str, ExecutionProfile]) -> dict[str, ExecutionProfile]:
        import re

        if any(re.fullmatch(NAME_PATTERN, name) is None for name in value):
            raise ValueError("Invalid execution profile name")
        return value

    @model_validator(mode="after")
    def execution_dependencies(self) -> Self:
        import re

        for name, binding in self.agent_bindings.items():
            profile = self.profiles.get(name)
            if profile is None or profile.kind != "agent" or profile.runtime_digest != binding.compatibility["runtime_digest"]:
                raise ValueError("Agent routing binding requires its pinned agent profile")
            if "*" in binding.allowed_user_ids:
                raise ValueError("Agent routing grants require explicit user identities")
        for slot, profile_name in self.scheduled_job_slots.items():
            if re.fullmatch(NAME_PATTERN, slot) is None:
                raise ValueError("Invalid scheduled job slot name")
            profile = self.profiles.get(profile_name)
            if profile is None or profile.kind != "job":
                raise ValueError("Scheduled job slots require an approved job profile")
        if self.renew_seconds * 2 >= self.lease_seconds:
            raise ValueError("Renew interval must be less than half the lease")
        if self.jobs_enabled and not self.enabled:
            raise ValueError("Jobs require Fleet enabled")
        if self.agents_enabled and not self.jobs_enabled:
            raise ValueError("Agents require jobs enabled")
        if self.continuations_enabled:
            jobs = [name for name, profile in self.profiles.items() if profile.kind == "job"]
            if self.reserved_job_profile is None:
                if len(jobs) != 1:
                    raise ValueError("Continuations require an explicit standard reserved job profile")
                object.__setattr__(self, "reserved_job_profile", jobs[0])
            elif self.reserved_job_profile not in jobs:
                raise ValueError("Reserved job profile must be an approved job profile")
        if self.continuations_enabled and not self.agents_enabled:
            raise ValueError("Continuations require agents enabled")
        if self.enabled and self.jobs_enabled:
            if self.nas_root is None or not self.nas_root.is_absolute():
                raise ValueError("Enabled jobs require an absolute NAS root")
            if self.nas_identity is None:
                raise ValueError("Enabled jobs require an explicit NAS deployment identity")
            if not any(profile.kind == "job" for profile in self.profiles.values()):
                raise ValueError("Enabled jobs require at least one explicit job profile")
        if self.agents_enabled and not any(profile.kind == "agent" for profile in self.profiles.values()):
            raise ValueError("Enabled agents require at least one explicit agent profile")
        return self

    def validate_host(self, *, database_backend: str) -> None:
        if self.enabled and database_backend not in {"postgres", "postgresql"}:
            raise ValueError("Enabled Fleet requires Postgres")
