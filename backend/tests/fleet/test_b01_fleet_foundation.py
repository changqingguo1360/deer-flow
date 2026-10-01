"""Configuration and wire contracts required before any Fleet execution."""

import importlib
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError


def fleet_module(name):
    assert importlib.util.find_spec("deerflow_ecs_fleet") is not None, "The optional Fleet package has not been implemented"
    return importlib.import_module(f"deerflow_ecs_fleet.{name}")


def enabled_config(**overrides):
    data = {
        "enabled": True,
        "jobs_enabled": True,
        "nas_root": "/srv/deerflow-data",
        "nas_identity": "fleet-test",
        "profiles": {
            "batch-standard": {
                "kind": "job",
                "image": "sha256:" + "a" * 64,
                "cpu_millis": 1000,
                "memory_mib": 2048,
            },
        },
    }
    return data | overrides


def test_b01_contract():
    config_cls = fleet_module("config").FleetConfig
    config = config_cls()
    assert not config.enabled
    assert not config.jobs_enabled
    assert not config.agents_enabled
    assert not config.continuations_enabled
    for fields in ({"lease_seconds": 60, "renew_seconds": 30}, {"agents_enabled": True}, {"continuations_enabled": True}):
        with pytest.raises(ValidationError):
            config_cls(**fields)


@pytest.mark.parametrize("field,value", [("lease_seconds", 0), ("renew_seconds", 0), ("queue_timeout_seconds", -1), ("staged_timeout_seconds", 0), ("unknown_field", 1)])
def test_invalid_timing_and_unknown_config_fail_closed(field, value):
    with pytest.raises(ValidationError):
        fleet_module("config").FleetConfig(**{field: value})


@pytest.mark.parametrize("fields", [{"jobs_enabled": True}, {"enabled": True, "jobs_enabled": True}, enabled_config(profiles={}), enabled_config(nas_root="relative/path")])
def test_enabled_jobs_require_explicit_storage_and_profiles(fields):
    with pytest.raises(ValidationError):
        fleet_module("config").FleetConfig(**fields)


def test_enabled_config_requires_postgres_host():
    config = fleet_module("config").FleetConfig(**enabled_config())
    with pytest.raises(ValueError, match="Postgres"):
        config.validate_host(database_backend="sqlite")
    config.validate_host(database_backend="postgres")


@pytest.mark.parametrize("changes", [{"cpu_millis": 0}, {"memory_mib": -1}, {"image": "alpine:latest"}, {"execution_timeout_seconds": 0}, {"network": "anything"}, {"unknown_key": "x"}])
def test_profile_rejects_unbounded_or_unpinned_execution(changes):
    profile = enabled_config()["profiles"]["batch-standard"] | changes
    with pytest.raises(ValidationError):
        fleet_module("config").ExecutionProfile(**profile)


def test_profile_can_only_reduce_requested_execution_budget():
    config = fleet_module("config").FleetConfig(**enabled_config())
    profile = config.profiles["batch-standard"]
    assert profile.execution_timeout_seconds == 1800
    assert profile.network == "none"
    assert profile.max_output_bytes > 0


def test_disabled_install_is_inert_and_does_not_import_harness():
    fleet_module("config")
    package_root = Path(__file__).resolve().parents[2] / "packages" / "ecs-fleet"
    env = os.environ | {"PYTHONPATH": str(package_root), "PYTHONDONTWRITEBYTECODE": "1"}
    result = subprocess.run(
        [sys.executable, "-c", "import sys; from deerflow_ecs_fleet import install; install(object(), {}); assert not any(n == 'deerflow' or n.startswith('deerflow.') for n in sys.modules)"],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_protocol_rejects_identity_injection_and_path_escape():
    spec_cls = fleet_module("protocol").JobSpec
    data = {"task_name": "Report", "profile": "batch-standard", "argv": ["python", "/inputs/analyze.py"], "input_manifests": []}
    spec = spec_cls(**data)
    assert spec.schema_version == 1
    assert spec.link_mode == "detached"
    for changes in ({"user_id": "other-user"}, {"argv": []}, {"schema_version": 999}, {"profile": "../admin"}, {"input_manifests": ["../other"]}):
        with pytest.raises(ValidationError):
            spec_cls(**(data | changes))
