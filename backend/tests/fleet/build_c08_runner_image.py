"""Fresh C08 installed inputs; preparation never runs Docker or replaces C07."""

import argparse
import configparser
import hashlib
import json
import shutil
import subprocess
import zipfile
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def prepare_context(context, *, dependency_tag="deerflow-c08-dependencies:local"):
    context = Path(context).resolve()
    context.mkdir(parents=True, exist_ok=False)
    shutil.copytree(FIXTURES / "c04-runner", context, dirs_exist_ok=True)
    for name in ("Dockerfile", "Dockerfile.provider"):
        path = context / name
        path.write_text(path.read_text().replace("deerflow-c04-dependencies:local", dependency_tag))
    path = context / "Dockerfile"
    dockerfile = path.read_text().replace("COPY libexec_bootstrap.py model-bindings.json runtime-bundle.json", "COPY libexec_bootstrap.py model-bindings.json runtime-bundle.json libexec_workspace_collector.py workspace-contracts.json")
    dockerfile = dockerfile.replace(
        "COPY c04_worker_fixture.py c04_mcp_fixture.py c04_tool_probe.py",
        "COPY c04_worker_fixture.py c04_mcp_fixture.py c04_tool_probe.py c07_integration_fixture.py "
        "c08_integration_fixture.py c08_linux_fixture.py c08_linux_probe.py c08_stock_linux_fixture.py c08_continuation_fixture.py c08_installed_bytes.py",
    )
    dockerfile += "\nCOPY wheel-inventory.json /opt/deerflow/wheel-inventory.json\n"
    path.write_text(dockerfile)
    for name in (
        "c04_worker_fixture.py",
        "c04_mcp_fixture.py",
        "c04_tool_probe.py",
        "c07_integration_fixture.py",
        "c08_integration_fixture.py",
        "c08_linux_fixture.py",
        "c08_linux_probe.py",
        "c08_stock_linux_fixture.py",
        "c08_continuation_fixture.py",
        "c08_installed_bytes.py",
    ):
        shutil.copyfile(Path(__file__).parent / name, context / name)
    worker = BACKEND / "packages/ecs-fleet/deerflow_ecs_fleet/worker"
    shutil.copyfile(worker / "libexec_bootstrap.py", context / "libexec_bootstrap.py")
    shutil.copyfile(worker / "workspace_collector.py", context / "libexec_workspace_collector.py")
    bundle = json.loads((context / "runtime-bundle.json").read_bytes())
    contracts = {
        "schema_version": 1,
        "host": {"use": "app.fleet.runner_context:build_agent_environment", "sandbox_use": "deerflow.sandbox.local:LocalSandboxProvider", "contract": "host-supervised"},
        "plugins": [{**item, "contract": "fenced-no-retained-user-data-writer"} for item in bundle["plugins"]],
        "mcp_servers": {
            name: {
                "binding": binding,
                "secret_bindings": {key: value for key, value in bundle["secret_bindings"].items() if value["kind"] == "mcp" and value["target"] == name},
                "contract": "stateless-reconnectable-no-retained-user-data-writer",
            }
            for name, binding in bundle["mcp_servers"].items()
        },
    }
    (context / "workspace-contracts.json").write_text(json.dumps(contracts, sort_keys=True, separators=(",", ":")))
    inputs = {str(path.relative_to(context)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(context.rglob("*")) if path.is_file()}
    (context / "prepared-inputs.json").write_text(json.dumps(inputs, indent=2))
    return context


def prepare_c07_variant(stock_context, destination):
    """Copy the same frozen wheels into an independently bound C07 runtime."""
    destination = Path(destination).resolve()
    destination.mkdir(parents=True, exist_ok=False)
    shutil.copytree(stock_context, destination, dirs_exist_ok=True)
    bundle = json.loads((destination / "runtime-bundle.json").read_bytes())
    bundle.update(plugins=[], mcp_servers={}, secret_bindings={})
    (destination / "runtime-bundle.json").write_text(json.dumps(bundle, sort_keys=True, separators=(",", ":")))
    bindings = {"model-1": {"provider_use": "fleet.c07_integration_fixture:BarrierModel", "target_model": "c07", "version": "v1"}}
    (destination / "model-bindings.json").write_text(json.dumps(bindings, sort_keys=True, separators=(",", ":")))
    contracts = json.loads((destination / "workspace-contracts.json").read_bytes())
    contracts.update(plugins=[], mcp_servers={})
    (destination / "workspace-contracts.json").write_text(json.dumps(contracts, sort_keys=True, separators=(",", ":")))
    for name in ("prepared-inputs.json", "completed-build-inputs.json"):
        (destination / name).unlink(missing_ok=True)
    inputs = {str(path.relative_to(destination)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(destination.rglob("*")) if path.is_file()}
    (destination / "completed-build-inputs.json").write_text(json.dumps(inputs, indent=2, sort_keys=True))
    return destination


def wheel_inventory(context):
    """Freeze every wheel member, including metadata and declared entrypoints."""
    inventory = {}
    for path in sorted(Path(context).glob("*.whl")):
        with zipfile.ZipFile(path) as archive:
            members = {name: hashlib.sha256(archive.read(name)).hexdigest() for name in sorted(archive.namelist()) if not name.endswith("/")}
            entrypoints = {}
            for name in members:
                if name.endswith(".dist-info/entry_points.txt"):
                    parser = configparser.ConfigParser(interpolation=None)
                    parser.optionxform = str
                    parser.read_string(archive.read(name).decode("utf-8"))
                    entrypoints.update({section: dict(parser.items(section)) for section in parser.sections()})
        inventory[path.name] = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "members": members, "entrypoints": entrypoints}
    return inventory


def run(*args):
    subprocess.run(args, cwd=BACKEND, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--c07-context", type=Path)
    parser.add_argument("--c07-tag")
    parser.add_argument("--dependency-tag", default="deerflow-c08-dependencies:local")
    parser.add_argument("--provider-tag", default="deerflow-c08-provider:local")
    args = parser.parse_args()
    if bool(args.c07_context) != bool(args.c07_tag):
        parser.error("--c07-context and --c07-tag must be supplied together")
    context = prepare_context(args.context, dependency_tag=args.dependency_tag)
    if args.prepare_only:
        return
    run("uv", "export", "--frozen", "--no-dev", "--extra", "postgres", "--extra", "redis", "--no-emit-workspace", "--no-header", "--format", "requirements-txt", "--output-file", str(context / "requirements.txt"))
    run("docker", "build", "-f", str(context / "Dockerfile.dependencies"), "-t", args.dependency_tag, str(context))
    for options in (
        (),
        ("--package", "deerflow-harness"),
        ("--package", "deerflow-extension-api"),
        ("--project", "packages/ecs-fleet"),
        ("--project", str(FIXTURES / "c04-runtime-plugin")),
        ("--project", str(FIXTURES / "c04-alternate-provider")),
    ):
        run("uv", "build", "--wheel", *options, "--out-dir", str(context))
    inventory = wheel_inventory(context)
    if len(inventory) != 6:
        raise ValueError("C08 installed acceptance requires exactly six freshly built wheels")
    (context / "wheel-inventory.json").write_text(json.dumps(inventory, indent=2, sort_keys=True))
    completed_inputs = {str(path.relative_to(context)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(context.rglob("*")) if path.is_file()}
    (context / "completed-build-inputs.json").write_text(json.dumps(completed_inputs, indent=2, sort_keys=True))
    run("docker", "build", "-f", str(context / "Dockerfile.provider"), "-t", args.provider_tag, str(context))
    run("docker", "build", "-t", args.tag, str(context))
    run("docker", "image", "inspect", "--format", "{{.Id}}", args.tag)
    if args.c07_context is not None:
        variant = prepare_c07_variant(context, args.c07_context)
        run("docker", "build", "-t", args.c07_tag, str(variant))
        run("docker", "image", "inspect", "--format", "{{.Id}}", args.c07_tag)


if __name__ == "__main__":
    main()
