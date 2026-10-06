#!/usr/bin/env python3
"""Run the two explicit production C operations; every case must pass without skips."""

import argparse
import ast
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

CASES = (
    "test_c12_production_gateway_runner_main",
    "test_c12_production_runner_revocation_and_delivery_recovery",
)
MODULE = "tests.fleet.test_c12_remote_agent_operations"


def validate_environment(environ):
    from sqlalchemy.engine import make_url

    uri = environ.get("DEERFLOW_TEST_POSTGRES_URL", "")
    if not uri.startswith("postgresql"):
        raise ValueError("C gate requires explicit isolated DEERFLOW_TEST_POSTGRES_URL")
    alternate = environ.get("TEST_POSTGRES_URI")
    if alternate and make_url(alternate).set(drivername="postgresql") != make_url(
        uri
    ).set(drivername="postgresql"):
        raise ValueError(
            "C gate PostgreSQL environment variables identify different databases"
        )
    if not Path("/opt/homebrew/bin/redis-server").is_file():
        raise ValueError(
            "C gate requires the existing local redis-server used by its owned fixture"
        )
    if not re.fullmatch(
        r"sha256:[a-f0-9]{64}", environ.get("FLEET_AGENT_TEST_IMAGE", "")
    ):
        raise ValueError("C gate requires the production Agent image content ID")
    docker = Path(environ.get("C12_DOCKER", ""))
    if not docker.is_absolute() or not docker.is_file():
        raise ValueError("C gate requires an existing absolute C12_DOCKER executable")
    evidence = Path(environ.get("C12_EVIDENCE_DIR", ""))
    if not evidence.is_absolute():
        raise ValueError("C gate requires an absolute C12_EVIDENCE_DIR")


def validate_report(path, required_cases=CASES):
    cases = list(ET.parse(path).getroot().iter("testcase"))
    observed = [(case.get("classname"), case.get("name")) for case in cases]
    expected = [(MODULE, name) for name in required_cases]
    failed = sum(
        case.find("failure") is not None or case.find("error") is not None
        for case in cases
    )
    skipped = sum(case.find("skipped") is not None for case in cases)
    if sorted(observed) != sorted(expected) or failed or skipped:
        raise ValueError(
            f"C gate rejected report: collected={len(cases)}, failed={failed}, skipped={skipped}, required={list(CASES)}"
        )
    return {"collected": len(cases), "passed": len(cases), "skipped": skipped}


def verify_cleanup_qualification(root, directory, filename, expected):
    """Keep the old execution intact; only review-qualified main cleanup may differ."""
    target = "backend/tests/fleet/test_c12_remote_agent_operations.py"
    if filename != target:
        raise ValueError("Source changed after successful fault execution: " + filename)
    qualification = json.loads(
        (directory / "post-review-cleanup-qualification.json").read_text()
    )
    prior = directory / "post-review-original-test-source.py"
    current = root / filename
    if (
        qualification["source"] != target
        or qualification["quality_review"] != "Ready"
        or qualification["executed_sha256"] != expected
        or hashlib.sha256(prior.read_bytes()).hexdigest() != expected
        or hashlib.sha256(current.read_bytes()).hexdigest()
        != qualification["reviewed_sha256"]
    ):
        raise ValueError("Cleanup qualification lacks matching execution/review source")

    def without_main_cleanup(path):
        module = ast.parse(path.read_text())
        main = next(
            node
            for node in module.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == CASES[0]
        )
        cleanups = [
            node
            for node in ast.walk(main)
            if isinstance(node, ast.Try)
            and any(
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id == "settle_owned_containers"
                for statement in node.finalbody
                for call in ast.walk(statement)
            )
        ]
        if len(cleanups) != 1:
            raise ValueError("Cleanup qualification is not one main finally")
        cleanups[0].finalbody = [ast.Pass()]
        return ast.dump(module, include_attributes=False)

    if without_main_cleanup(prior) != without_main_cleanup(current):
        raise ValueError("Source delta exceeds the reviewed main cleanup finally")
    return qualification


def verify_receipts(directory):
    """Validate prior natural executions; this mode does not execute pytest."""
    root = Path(__file__).resolve().parents[1]
    loaded = {}
    inputs = {}
    for attempt, case in zip(
        ("main-attempt-06", "fault-attempt-05"), CASES, strict=True
    ):
        receipt_path = directory / attempt / "command-receipt.json"
        receipt = json.loads(receipt_path.read_text())
        command = receipt.get("argv", receipt.get("command", []))
        if receipt.get("natural_exit") != 0 or not any(
            value.endswith("::" + case) for value in command
        ):
            raise ValueError(
                "Prior execution receipt missing natural success: " + attempt
            )
        if (
            not Path(command[0]).is_absolute()
            or Path(receipt["cwd"]) != root / "backend"
        ):
            raise ValueError("Prior execution command is not concrete: " + attempt)
        cleanup = json.loads((directory / attempt / "owned-schema.json").read_text())
        if cleanup.get("created") is not True or cleanup.get("dropped") is not True:
            raise ValueError("Owned schema cleanup missing: " + attempt)
        log = (directory / attempt / "test.log").read_text()
        if not re.search(r"1 passed(?:,| in)", log) or re.search(r"\d+ skipped", log):
            raise ValueError(
                "Prior execution log does not prove one pass without skips"
            )
        inputs[str(receipt_path)] = hashlib.sha256(
            receipt_path.read_bytes()
        ).hexdigest()
        loaded[attempt] = receipt
    freeze = json.loads((directory / "fault-green-source-freeze.json").read_text())
    main_freeze = json.loads((directory / "main-green-source-freeze.json").read_text())
    for filename in (
        "backend/app/fleet/runtime.py",
        "backend/app/gateway/deps.py",
    ):
        if main_freeze["source_sha256"][filename] != freeze["source"][filename]:
            raise ValueError(
                "Runtime participating source differs between prior main/fault proofs: "
                + filename
            )
    source_qualifications = []
    for filename, expected in freeze["source"].items():
        if hashlib.sha256((root / filename).read_bytes()).hexdigest() != expected:
            source_qualifications.append(
                verify_cleanup_qualification(root, directory, filename, expected)
            )
    main = json.loads(
        (directory / "main-attempt-06/main-observations.json").read_text()
    )
    build = json.loads((directory / "production-build-receipt.json").read_text())
    if main["image"] != build["image_id"] or build["command_exit"] != 0:
        raise ValueError("Production image identity/build success mismatch")
    audit = json.loads(
        (directory / "fault-attempt-05/installed-source-audit.json").read_text()
    )
    artifacts = directory / "production-artifacts"
    manifest_path = artifacts / "frozen-inputs.json"
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    if manifest_sha256 != build["frozen_inputs_sha256"]:
        raise ValueError("Frozen input manifest differs from successful build receipt")
    frozen = json.loads(manifest_path.read_text())
    required_artifacts = {
        "requirements.lock",
        "closure-audit.json",
        "model-bindings.json",
        "runtime-bundle.json",
        "workspace-contracts.json",
    }
    if not required_artifacts <= frozen.keys():
        raise ValueError("Frozen input manifest missing required production artifacts")
    for filename, expected in frozen.items():
        if hashlib.sha256((artifacts / filename).read_bytes()).hexdigest() != expected:
            raise ValueError("Frozen build artifact changed: " + filename)
    closure_path = directory / build["closure_audit"]
    if closure_path.resolve() != (artifacts / "closure-audit.json").resolve():
        raise ValueError("Build receipt references a different closure audit")
    closure = json.loads(closure_path.read_text())
    recipe = root / "docker/fleet/agent.Dockerfile"
    recipe_sha256 = hashlib.sha256(recipe.read_bytes()).hexdigest()
    if (
        recipe_sha256 != build["recipe_sha256"]
        or closure["recipe_sha256"] != recipe_sha256
    ):
        raise ValueError(
            "Original production recipe differs from successful build inputs"
        )
    if (
        closure["base"] != build["base"]
        or build["base"] not in closure["base_repo_digests"]
        or not re.fullmatch(r"sha256:[a-f0-9]{64}", closure["base_image_id"])
        or not closure["base_installed"]["distributions"]
    ):
        raise ValueError(
            "Frozen base identity/inventory differs from successful build receipt"
        )
    command = build["command"]
    if (
        "--network=none" not in command
        or "--pull=false" not in command
        or "AGENT_BASE=" + build["base"] not in command
        or command[command.index("-f") + 1] != str(recipe)
        or "agent_artifacts=" + str(artifacts) not in command
    ):
        raise ValueError(
            "Build receipt does not use the frozen offline production recipe/base/artifacts"
        )
    for wheel in closure["wheels"].values():
        if frozen.get("wheelhouse/" + wheel["filename"]) != wheel["sha256"]:
            raise ValueError("Closure wheel is not bound by the frozen manifest")
    members = 0
    observed_projects = set()
    required_projects = {
        "deer-flow",
        "deerflow-harness",
        "deerflow-extension-api",
        "deerflow-ecs-fleet",
    }
    copied_expected = {}
    for wheel in (artifacts / "wheelhouse").glob("*.whl"):
        if wheel.name.startswith("websockets-"):
            continue
        with zipfile.ZipFile(wheel) as archive:
            metadata = next(
                name
                for name in archive.namelist()
                if name.endswith(".dist-info/METADATA")
            )
            name = next(
                line.removeprefix("Name: ")
                for line in archive.read(metadata).decode().splitlines()
                if line.startswith("Name: ")
            )
            if name not in required_projects or name in observed_projects:
                raise ValueError(
                    "Unexpected or duplicate production distribution wheel: " + name
                )
            observed_projects.add(name)
            expected = {
                member: hashlib.sha256(archive.read(member)).hexdigest()
                for member in archive.namelist()
                if member.endswith(".py")
            }
            if not expected or audit["distributions"][name]["members"] != expected:
                raise ValueError("Installed bytes mismatch: " + name)
            members += len(expected)
            if name == "deerflow-ecs-fleet":
                copied_expected = {
                    "/opt/deerflow/libexec_bootstrap.py": expected[
                        "deerflow_ecs_fleet/worker/libexec_bootstrap.py"
                    ],
                    "/opt/deerflow/libexec_workspace_collector.py": expected[
                        "deerflow_ecs_fleet/worker/workspace_collector.py"
                    ],
                }
    if (
        observed_projects != required_projects
        or members == 0
        or set(audit["distributions"]) != required_projects
        or audit["copied_files"] != copied_expected
    ):
        raise ValueError("Incomplete installed distribution/copy audit")
    frozen = json.loads((artifacts / "frozen-inputs.json").read_text())
    if audit["approved_assets"] != {
        name: frozen[name]
        for name in (
            "model-bindings.json",
            "runtime-bundle.json",
            "workspace-contracts.json",
        )
    }:
        raise ValueError("Approved installed artifacts mismatch")
    return {
        "gate": "C",
        "mode": "verify_prior_receipts",
        "fresh_execution": False,
        "prior_natural_passes": 2,
        "skipped": 0,
        "installed_python_members": members,
        "image": build["image_id"],
        "source_hashes": freeze["source"],
        "post_execution_source_qualifications": source_qualifications,
        "main_executed_source_hashes": main_freeze["source_sha256"],
        "gate_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "frozen_inputs_sha256": manifest_sha256,
        "verified_frozen_artifacts": frozen,
        "build_receipt_sha256": hashlib.sha256(
            (directory / "production-build-receipt.json").read_bytes()
        ).hexdigest(),
        "recipe_sha256": recipe_sha256,
        "base": closure["base"],
        "base_image_id": closure["base_image_id"],
        "source_provenance": "Main source hashes belong to main-attempt-06; later fault fixture/format changes belong to fault-attempt-05. Gateway runtime participants match; later build-utility formatting is separate from original image build. Hashes are not attributed to a shared execution.",
        "receipt_hashes": inputs,
        "proof_scope": "Prior actual main and concentrated fault executions; stateless noop and initialized Store. Native transactional memory/Store/Redis evidence requires separately qualified reuse; not executed by this gate.",
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        type=Path,
        help="Retain a JSON manifest and two actual per-case JUnit XML files at new paths",
    )
    parser.add_argument(
        "--verify-receipts",
        type=Path,
        help="Verify the two recorded prior natural executions; does not execute pytest",
    )
    args = parser.parse_args(argv)
    if args.verify_receipts:
        try:
            print(
                json.dumps(
                    verify_receipts(args.verify_receipts.resolve()), sort_keys=True
                )
            )
        except (ValueError, OSError, KeyError) as error:
            print(str(error), file=sys.stderr)
            return 1
        return 0
    try:
        validate_environment(os.environ)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    backend = Path(__file__).resolve().parents[1] / "backend"
    evidence = Path(os.environ["C12_EVIDENCE_DIR"])
    evidence.mkdir(parents=True, exist_ok=False)
    preflight = [
        [os.environ["C12_DOCKER"], "info", "--format", "{{.ServerVersion}}"],
        [
            os.environ["C12_DOCKER"],
            "image",
            "inspect",
            os.environ["FLEET_AGENT_TEST_IMAGE"],
        ],
        [
            sys.executable,
            "-c",
            "import asyncio,os\nfrom sqlalchemy import text\nfrom sqlalchemy.engine import make_url\nfrom sqlalchemy.ext.asyncio import create_async_engine\nasync def check():\n    engine=create_async_engine(make_url(os.environ['DEERFLOW_TEST_POSTGRES_URL']).set(drivername='postgresql+asyncpg'))\n    try:\n        async with engine.connect() as connection:\n            await connection.execute(text('SELECT 1'))\n    finally:\n        await engine.dispose()\nasyncio.run(check())\n",
        ],
    ]
    for command in preflight:
        result = subprocess.run(command, capture_output=True, check=False)
        if result.returncode:
            print("C gate prerequisite unavailable; no cases executed", file=sys.stderr)
            return 2
    executions = []
    with tempfile.TemporaryDirectory(prefix="fleet-c-gate-") as temporary:
        for index, case in enumerate(CASES):
            report = Path(temporary) / (str(index) + ".xml")
            case_evidence = evidence / case
            case_evidence.mkdir()
            command = [
                sys.executable,
                "-m",
                "pytest",
                "tests/fleet/test_c12_remote_agent_operations.py::" + case,
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
            environ["C12_EVIDENCE_DIR"] = str(case_evidence)
            record = {"argv": command, "cwd": str(backend)}
            (case_evidence / "command-start.json").write_text(
                json.dumps(record, indent=2)
            )
            with (case_evidence / "test.log").open("w") as log:
                result = subprocess.run(
                    command,
                    cwd=backend,
                    env=environ,
                    check=False,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
            record["natural_exit"] = result.returncode
            (case_evidence / "command-receipt.json").write_text(
                json.dumps(record, indent=2)
            )
            if report.exists():
                (case_evidence / "pytest.xml").write_bytes(report.read_bytes())
            if result.returncode:
                return result.returncode
            try:
                counts = validate_report(report, required_cases=(case,))
            except (ValueError, OSError, ET.ParseError) as error:
                print(str(error), file=sys.stderr)
                return 1
            executions.append(
                {
                    **record,
                    **counts,
                    "evidence": str(case_evidence),
                    "junit": str(case_evidence / "pytest.xml"),
                }
            )
        summary = {
            "gate": "C",
            "mode": "fresh_execution",
            "fresh_execution": True,
            "executions": executions,
            "collected": 2,
            "passed": 2,
            "skipped": 0,
        }
        if args.report:
            with args.report.open("x") as retained:
                json.dump(summary, retained, indent=2)
        print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
