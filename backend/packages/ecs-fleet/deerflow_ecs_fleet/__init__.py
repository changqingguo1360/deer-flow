"""Optional Fleet extension. Disabled installs never import the host runtime."""

from collections.abc import Mapping
from typing import Any

from .config import FleetConfig


def install(registry: Any, config: Mapping[str, Any]) -> None:
    settings = FleetConfig.model_validate(dict(config))
    if not settings.enabled:
        return
    from deerflow.config import get_app_config

    settings.validate_host(database_backend=get_app_config().database.backend)
    from .service import FleetService

    registry.service(FleetService(settings))
