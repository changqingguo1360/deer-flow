"""Prepare/build a separate C07 installed image; preserve B/C06 images."""

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def run(*args):
    subprocess.run(args, cwd=BACKEND, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    context = args.context.resolve()
    context.mkdir(parents=True, exist_ok=False)
    shutil.copytree(FIXTURES / "c04-runner", context, dirs_exist_ok=True)
    for name in ("Dockerfile", "Dockerfile.provider"):
        path = context / name
        path.write_text(path.read_text().replace("deerflow-c04-dependencies:local", "deerflow-c07-dependencies:local"))
    path = context / "Dockerfile"
    path.write_text(path.read_text().replace("COPY c04_worker_fixture.py c04_mcp_fixture.py c04_tool_probe.py", "COPY c04_worker_fixture.py c04_mcp_fixture.py c04_tool_probe.py c07_integration_fixture.py"))
    for name in ("c04_worker_fixture.py", "c04_mcp_fixture.py", "c04_tool_probe.py", "c07_integration_fixture.py"):
        shutil.copyfile(Path(__file__).parent / name, context / name)
    shutil.copyfile(BACKEND / "packages/ecs-fleet/deerflow_ecs_fleet/worker/libexec_bootstrap.py", context / "libexec_bootstrap.py")
    inputs = {str(path.relative_to(context)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(context.rglob("*")) if path.is_file()}
    (context / "prepared-inputs.json").write_text(json.dumps(inputs, indent=2))
    if args.prepare_only:
        return
    run("uv", "export", "--frozen", "--no-dev", "--extra", "postgres", "--extra", "redis", "--no-emit-workspace", "--no-header", "--format", "requirements-txt", "--output-file", str(context / "requirements.txt"))
    run("docker", "build", "-f", str(context / "Dockerfile.dependencies"), "-t", "deerflow-c07-dependencies:local", str(context))
    for options in (
        (),
        ("--package", "deerflow-harness"),
        ("--package", "deerflow-extension-api"),
        ("--project", "packages/ecs-fleet"),
        ("--project", str(FIXTURES / "c04-runtime-plugin")),
        ("--project", str(FIXTURES / "c04-alternate-provider")),
    ):
        run("uv", "build", "--wheel", *options, "--out-dir", str(context))
    run("docker", "build", "-f", str(context / "Dockerfile.provider"), "-t", "deerflow-c07-provider:local", str(context))
    run("docker", "build", "-t", args.tag, str(context))
    run("docker", "image", "inspect", "--format", "{{.Id}}", args.tag)


if __name__ == "__main__":
    main()
