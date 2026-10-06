"""Public execution intent carries no placement or ownership authority."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ExecutionPreference(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    preference: Literal["local", "remote", "auto"] = "auto"
    profile: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$")
