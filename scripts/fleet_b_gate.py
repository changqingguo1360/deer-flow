"""Run the explicit local B gate; absent prerequisites or any skip fail closed."""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path


def validate_environment(environ):
    if not environ.get("TEST_POSTGRES_URI", "").startswith("postgresql+asyncpg://"):
        raise ValueError(
            "B gate requires explicit TEST_POSTGRES_URI (isolated PostgreSQL)"
        )
    if environ.get("FLEET_TEST_CONTAINERS") != "1":
        raise ValueError("B gate requires FLEET_TEST_CONTAINERS=1")
    if not re.fullmatch(
        r"sha256:[a-f0-9]{64}", environ.get("FLEET_TEST_WORKER_IMAGE", "")
    ):
        raise ValueError(
            "B gate requires a locally built FLEET_TEST_WORKER_IMAGE content ID"
        )
    if not Path(environ.get("FLEET_TEST_DOCKER_SOCKET", "")).is_absolute():
        raise ValueError(
            "B gate requires an explicit absolute FLEET_TEST_DOCKER_SOCKET"
        )

    if not re.fullmatch(
        r"[^\s@]+@sha256:[a-f0-9]{64}", environ.get("FLEET_WORKER_BASE", "")
    ):
        raise ValueError("B gate requires a frozen FLEET_WORKER_BASE image digest")
    if not re.fullmatch(r"[a-f0-9]{64}", environ.get("FLEET_DOCKER_CLI_SHA256", "")):
        raise ValueError("B gate requires a verified FLEET_DOCKER_CLI_SHA256")
    artifacts = Path(environ.get("FLEET_BUILD_ARTIFACTS", ""))
    if not artifacts.is_absolute() or not artifacts.is_dir():
        raise ValueError(
            "B gate requires FLEET_BUILD_ARTIFACTS as an absolute existing directory"
        )


def validate_report(path, *, required_modules=()):
    root = ET.parse(path).getroot()
    cases = list(root.iter("testcase"))
    skipped = sum(case.find("skipped") is not None for case in cases)
    failed = sum(
        case.find("failure") is not None or case.find("error") is not None
        for case in cases
    )
    if not cases or skipped or failed:
        raise ValueError(
            f"B gate rejected report: collected={len(cases)}, skipped={skipped}, failed={failed}"
        )
    observed = {case.get("classname", "") for case in cases}
    missing = {
        module
        for module in required_modules
        if not any(name == module or name.startswith(module + ".") for name in observed)
    }
    if missing:
        raise ValueError(
            "B gate report missing required modules: " + ", ".join(sorted(missing))
        )
    return {"collected": len(cases), "passed": len(cases), "skipped": skipped}


def select_test_files(directory):
    return sorted(
        path
        for path in directory.glob("test_*.py")
        if re.fullmatch(r"test_b(?:0[1-9]|1[0-2])_.+\.py", path.name)
        or path.name == "test_b_acceptance.py"
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        type=Path,
        help="Keep the actual pytest JUnit report at this new path",
    )
    args = parser.parse_args(argv)
    try:
        validate_environment(os.environ)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    backend = Path(__file__).resolve().parents[1] / "backend"
    files = select_test_files(backend / "tests/fleet")
    for phase in range(1, 13):
        if not any(path.name.startswith(f"test_b{phase:02}_") for path in files):
            print(f"B gate missing phase B{phase:02} tests", file=sys.stderr)
            return 2
    selected = [str(path.relative_to(backend)) for path in files]
    required = {"tests.fleet." + path.stem for path in files}
    # An exclusive new report prevents passing a stale successful report after a failed run.
    with tempfile.TemporaryDirectory(prefix="fleet-b-gate-") as temporary:
        report = Path(temporary) / "pytest.xml"
        command = [
            sys.executable,
            "-m",
            "pytest",
            *selected,
            "-o",
            "addopts=",
            "-q",
            "-p",
            "no:cacheprovider",
            "--tb=short",
            "--show-capture=no",
            f"--junitxml={report}",
        ]
        environ = dict(os.environ)
        environ.pop("PYTEST_ADDOPTS", None)
        result = subprocess.run(command, cwd=backend, env=environ, check=False)
        if result.returncode:
            return result.returncode
        try:
            counts = validate_report(report, required_modules=required)
        except (ValueError, OSError, ET.ParseError) as error:
            print(str(error), file=sys.stderr)
            return 1
        if args.report:
            with args.report.open("xb") as retained:
                retained.write(report.read_bytes())
        print(json.dumps({"gate": "B", "command": command, **counts}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
