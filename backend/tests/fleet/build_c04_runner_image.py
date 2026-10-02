"""Build the independent acceptance image from frozen deps and real host wheels.

Run from any directory with the backend interpreter. Never rebuilds the B image.
"""

import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[2]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def run(*args):
    subprocess.run(args, cwd=BACKEND, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default="deerflow-c04-runner:local")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="fleet-c04-image-") as temporary:
        context = Path(temporary)
        shutil.copytree(FIXTURES / "c04-runner", context, dirs_exist_ok=True)
        run("uv", "export", "--frozen", "--no-dev", "--extra", "postgres", "--no-emit-workspace", "--format", "requirements-txt", "--output-file", str(context / "requirements.txt"))
        run("docker", "build", "-f", str(context / "Dockerfile.dependencies"), "-t", "deerflow-c04-dependencies:local", str(context))
        for options in (
            (),
            ("--package", "deerflow-harness"),
            ("--package", "deerflow-extension-api"),
            ("--project", "packages/ecs-fleet"),
            ("--project", str(FIXTURES / "c04-runtime-plugin")),
            ("--project", str(FIXTURES / "c04-alternate-provider")),
        ):
            run("uv", "build", "--wheel", *options, "--out-dir", str(context))
        for name in ("c04_worker_fixture.py", "c04_mcp_fixture.py", "c04_tool_probe.py"):
            shutil.copyfile(Path(__file__).parent / name, context / name)
        shutil.copyfile(BACKEND / "packages/ecs-fleet/deerflow_ecs_fleet/worker/libexec_bootstrap.py", context / "libexec_bootstrap.py")
        run("docker", "build", "-f", str(context / "Dockerfile.provider"), "-t", "deerflow-c04-provider:local", str(context))
        run("docker", "build", "-t", args.tag, str(context))
        run("docker", "image", "inspect", "--format", "{{.Id}}", args.tag)


if __name__ == "__main__":
    main()
