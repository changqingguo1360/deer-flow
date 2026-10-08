#!/usr/bin/env python3
"""Freeze approved local Agent artifacts and build the production offline recipe.

Accepts operator-selected wheels/assets and a digest-pinned cached base. It
never resolves versions, downloads dependencies or chooses a model provider.
"""

import argparse
import hashlib
import json
import subprocess
import zipfile
from email.parser import BytesParser
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

PROJECTS = {
    "deer-flow",
    "deerflow-harness",
    "deerflow-extension-api",
    "deerflow-ecs-fleet",
}
ROOT = Path(__file__).resolve().parents[1]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(argv):
    result = subprocess.run(argv, check=True, text=True, capture_output=True)
    return result.stdout


def prepare(args):
    if "@sha256:" not in args.base:
        raise ValueError("Agent base must be pinned by repository digest")
    artifacts = args.artifacts.resolve()
    wheels = sorted((artifacts / "wheelhouse").glob("*.whl"))
    if not wheels:
        raise ValueError("No approved local wheels supplied")
    inventory = {}
    for wheel in wheels:
        with zipfile.ZipFile(wheel) as archive:
            metadata = BytesParser().parsebytes(
                archive.read(
                    next(
                        name
                        for name in archive.namelist()
                        if name.endswith(".dist-info/METADATA")
                    )
                )
            )
            name = canonicalize_name(metadata["Name"])
            if name in inventory:
                raise ValueError("Ambiguous supplied distribution: " + name)
            inventory[name] = {
                "filename": wheel.name,
                "version": metadata["Version"],
                "sha256": sha256(wheel),
                "requires": metadata.get_all("Requires-Dist", []),
                "members": {
                    name: hashlib.sha256(archive.read(name)).hexdigest()
                    for name in archive.namelist()
                    if not name.endswith("/")
                },
            }
    if not PROJECTS <= inventory.keys():
        raise ValueError("Supply all four current production wheels")
    inspection = json.loads(command([args.docker, "image", "inspect", args.base]))[0]
    base = json.loads(
        command(
            [
                args.docker,
                "run",
                "--rm",
                "--pull=never",
                "--network=none",
                "--entrypoint",
                "python",
                args.base,
                "-c",
                'import json,importlib.metadata as m; from packaging.markers import default_environment; print(json.dumps({"environment":default_environment(),"distributions":{d.metadata["Name"]:{"version":d.version,"requires":d.requires or []} for d in m.distributions()}}))',
            ]
        )
    )
    installed = {
        canonicalize_name(name): value for name, value in base["distributions"].items()
    }
    closure = installed | inventory
    extras = {
        "deer-flow": {"postgres", "redis"},
        "deerflow-harness": {"postgres", "redis"},
        **{canonicalize_name(name): set() for name in args.approved_distribution},
    }
    pending = list(PROJECTS | set(extras))
    checked = set()
    failures = []
    while pending:
        name = pending.pop()
        selected_extras = extras.get(name, set())
        identity = (name, tuple(sorted(selected_extras)))
        if identity in checked:
            continue
        checked.add(identity)
        if name not in closure:
            failures.append("Missing approved distribution: " + name)
            continue
        for raw in closure[name]["requires"]:
            requirement = Requirement(raw)
            if requirement.marker and not any(
                requirement.marker.evaluate(base["environment"] | {"extra": extra})
                for extra in {""} | selected_extras
            ):
                continue
            target = canonicalize_name(requirement.name)
            if (
                target not in closure
                or closure[target]["version"] not in requirement.specifier
            ):
                failures.append(name + " requires " + str(requirement))
                continue
            previous = extras.setdefault(target, set()).copy()
            extras[target].update(requirement.extras)
            if (
                target,
                tuple(sorted(extras[target])),
            ) not in checked or previous != extras[target]:
                pending.append(target)
    if failures:
        raise ValueError(
            "Approved local closure is incomplete: " + "; ".join(sorted(set(failures)))
        )
    for name in (
        "model-bindings.json",
        "runtime-bundle.json",
        "workspace-contracts.json",
    ):
        json.loads((artifacts / name).read_bytes())
    if not (artifacts / "skills").is_dir():
        raise ValueError("Approved skills directory required")
    lock = []
    for name, item in sorted(inventory.items()):
        suffix = "[postgres,redis]" if name in {"deer-flow", "deerflow-harness"} else ""
        lock.append(
            name + suffix + "==" + item["version"] + " --hash=sha256:" + item["sha256"]
        )
    (artifacts / "requirements.lock").write_text("\n".join(lock) + "\n")
    proof = {
        "base": args.base,
        "base_image_id": inspection["Id"],
        "base_repo_digests": inspection.get("RepoDigests", []),
        "base_installed": base,
        "wheels": inventory,
        "approved_distributions": args.approved_distribution,
        "recipe_sha256": sha256(ROOT / "docker/fleet/agent.Dockerfile"),
    }
    (artifacts / "closure-audit.json").write_text(
        json.dumps(proof, indent=2, sort_keys=True)
    )
    files = {
        str(path.relative_to(artifacts)): sha256(path)
        for path in sorted(artifacts.rglob("*"))
        if path.is_file() and path.name not in {"SHA256SUMS", "frozen-inputs.json"}
    }
    (artifacts / "frozen-inputs.json").write_text(
        json.dumps(files, indent=2, sort_keys=True)
    )
    files["frozen-inputs.json"] = sha256(artifacts / "frozen-inputs.json")
    (artifacts / "SHA256SUMS").write_text(
        "".join(value + "  " + name + "\n" for name, value in sorted(files.items()))
    )


def build(args):
    artifacts = args.artifacts.resolve()
    frozen = json.loads((artifacts / "frozen-inputs.json").read_bytes())
    if any(sha256(artifacts / name) != value for name, value in frozen.items()):
        raise ValueError("Frozen approved artifacts changed")
    audit = json.loads((artifacts / "closure-audit.json").read_bytes())
    if audit["base"] != args.base or audit["recipe_sha256"] != sha256(
        ROOT / "docker/fleet/agent.Dockerfile"
    ):
        raise ValueError("Frozen base or production recipe changed")
    subprocess.run(
        [
            args.docker,
            "build",
            "--network=none",
            "--pull=false",
            "--build-context",
            "agent_artifacts=" + str(artifacts),
            "--build-arg",
            "AGENT_BASE=" + args.base,
            "-f",
            str(ROOT / "docker/fleet/agent.Dockerfile"),
            "-t",
            args.tag,
            str(ROOT / "docker/fleet"),
        ],
        check=True,
    )
    result = json.loads(command([args.docker, "image", "inspect", args.tag]))[0]
    args.receipt.write_text(
        json.dumps(
            {
                "image_id": result["Id"],
                "base": args.base,
                "recipe_sha256": audit["recipe_sha256"],
                "frozen_inputs_sha256": sha256(artifacts / "frozen-inputs.json"),
                "command_exit": 0,
            },
            indent=2,
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "build"))
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--approved-distribution", action="append", default=[])
    parser.add_argument("--tag")
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    if args.mode == "prepare":
        prepare(args)
    else:
        if not args.tag or not args.receipt:
            parser.error("build requires --tag and --receipt")
        build(args)


if __name__ == "__main__":
    main()
