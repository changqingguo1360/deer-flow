"""The explicit B gate fails closed on missing prerequisites or skipped tests."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("fleet_b_gate", ROOT / "scripts/fleet_b_gate.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)

ENV = {"TEST_POSTGRES_URI": "postgresql+asyncpg://test@127.0.0.1/test", "FLEET_TEST_CONTAINERS": "1", "FLEET_TEST_WORKER_IMAGE": "sha256:" + "a" * 64, "FLEET_TEST_DOCKER_SOCKET": "/tmp/docker.sock"}


@pytest.mark.parametrize("missing", list(ENV))
def test_explicit_gate_rejects_missing_prerequisites(missing):
    env = dict(ENV)
    del env[missing]
    with pytest.raises(ValueError):
        gate.validate_environment(env)


@pytest.mark.parametrize(
    "body",
    [
        "<testsuites/>",
        '<testsuites><testsuite tests="1" skipped="1"><testcase><skipped/></testcase></testsuite></testsuites>',
        '<testsuites><testsuite tests="1" failures="1"><testcase><failure/></testcase></testsuite></testsuites>',
        '<testsuites><testsuite tests="1" errors="1"><testcase><error/></testcase></testsuite></testsuites>',
    ],
)
def test_explicit_gate_rejects_empty_skipped_or_failed_report(tmp_path, body):
    report = tmp_path / "result.xml"
    report.write_text(body)
    with pytest.raises(ValueError):
        gate.validate_report(report)


def test_explicit_gate_counts_actual_testcases(tmp_path):
    gate.validate_environment(ENV)
    report = tmp_path / "result.xml"
    report.write_text('<testsuites><testsuite tests="999"><testcase name="one"/><testcase name="two"/></testsuite></testsuites>')
    assert gate.validate_report(report) == {"collected": 2, "passed": 2, "skipped": 0}


def test_gate_report_requires_every_selected_module(tmp_path):
    report = tmp_path / "result.xml"
    report.write_text('<testsuites><testsuite><testcase classname="tests.fleet.test_b12_fleet_job_integration" name="test_b12_contract"/></testsuite></testsuites>')
    with pytest.raises(ValueError, match="missing required"):
        gate.validate_report(report, required_modules={"tests.fleet.test_b01_fleet_foundation", "tests.fleet.test_b12_fleet_job_integration"})


def test_b_gate_selection_excludes_continuations_and_c(tmp_path):
    for name in ["test_b00_contract.py", "test_b01_contract.py", "test_b12_compose.py", "test_b13_contract.py", "test_b_acceptance.py", "test_bc_01_contract.py", "test_c01_contract.py"]:
        (tmp_path / name).touch()
    assert [path.name for path in gate.select_test_files(tmp_path)] == ["test_b01_contract.py", "test_b12_compose.py", "test_b_acceptance.py"]
