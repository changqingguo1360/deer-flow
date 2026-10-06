"""One Task4 fixture image; reuse immutable audited C09 build/cache mechanics."""

import argparse
import json
import shutil
from pathlib import Path

import build_c09_runner_image as audited

SOURCES = (
    "c09_stock_linux_partition_fixture.py",
    "build_c09_partition_runner_image.py",
    "test_c09_stock_linux_partition.py",
)


def prepare(args):
    context = audited.prepare(args)
    here = Path(__file__).parent
    for name in SOURCES:
        shutil.copyfile(here / name, context / name)
    dockerfile = context / "Dockerfile"
    dockerfile.write_text(dockerfile.read_text() + "\nCOPY c09_stock_linux_partition_fixture.py /usr/local/lib/python3.12/site-packages/fleet/\n")
    (context / "model-bindings.json").write_text(
        json.dumps(
            {
                "model-1": {
                    "provider_use": "fleet.c09_stock_linux_partition_fixture:PartitionModel",
                    "target_model": "parent",
                    "version": "v1",
                }
            },
            sort_keys=True,
        )
    )
    seed = {str(path.relative_to(context)): audited.digest(path) for path in sorted(context.rglob("*")) if path.is_file() and path.name != "c09-prepared-inputs.json"}
    (context / "c09-prepared-inputs.json").write_text(json.dumps(seed, indent=2, sort_keys=True))
    return context


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--source-ready", type=Path, required=True)
    parser.add_argument("--cache-receipt", type=Path, required=True)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--build-prepared", action="store_true")
    parser.add_argument("--wheels-prepared", action="store_true")
    args = parser.parse_args()
    audited.verify_approved_source(args.source_ready)
    if not args.build_prepared:
        prepare(args)
    for name in SOURCES:
        if audited.digest(args.context / name) != audited.digest(Path(__file__).with_name(name)):
            raise ValueError("Task4 prepared source changed: " + name)
    args.build_prepared = True
    audited.build(args)


if __name__ == "__main__":
    main()
