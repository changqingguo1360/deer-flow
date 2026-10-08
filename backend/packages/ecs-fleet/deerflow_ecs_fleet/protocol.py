"""Versioned, identity-free wire specifications for execution requests."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from .config import NAME_PATTERN


class JobSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    task_name: str = Field(min_length=1, max_length=128)
    profile: str = Field(pattern=NAME_PATTERN)
    argv: list[str] = Field(min_length=1, max_length=256)
    input_manifests: list[Annotated[str, Field(pattern=NAME_PATTERN)]] = Field(default_factory=list, max_length=64)
    code_artifact_id: str | None = Field(default=None, pattern=NAME_PATTERN)
    execution_timeout_seconds: int = Field(default=1800, gt=0)
    queue_timeout_seconds: int = Field(default=1800, gt=0)
    link_mode: Literal["detached", "awaited"] = "detached"
