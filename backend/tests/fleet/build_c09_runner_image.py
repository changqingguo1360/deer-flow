"""C09 single runner build: audited dependency/cache reuse, no dependency build."""

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

from build_c08_runner_image import BACKEND, FIXTURES, prepare_context, wheel_inventory

DEPENDENCY_TAG = "deerflow-c08-dependencies:diagnostic-stager-f6fcfa30dfb44e42be92e5958c41ea4a"
DEPENDENCY = "sha256:da399e40963d6d8ad1beff9bbe592e40a327f7417b14c6e929e4c61a4804ff5c"
CACHED_ROOTS = {
    "deerflow_extension_api": BACKEND / "packages/extension-api",
    "deerflow_c04_runtime_fixture": FIXTURES / "c04-runtime-plugin",
    "deerflow_c04_alternate_provider": FIXTURES / "c04-alternate-provider",
}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def checked_cached_wheels(receipt):
    proof = json.loads(Path(receipt).read_text())
    context = Path(proof["contexts"]["stock"])
    actual = wheel_inventory(context)
    found = {}
    for prefix, root in CACHED_ROOTS.items():
        names = [name for name in actual if name.startswith(prefix + "-")]
        if len(names) != 1:
            raise ValueError("Missing or ambiguous cached wheel: " + prefix)
        name = names[0]
        expected = proof["wheels"][name]
        for key in ("sha256", "members", "entrypoints"):
            if actual[name][key] != expected[key]:
                raise ValueError("Audited cached wheel changed: " + name)
        for member, value in actual[name]["members"].items():
            if member.endswith(".py") and digest(root / member) != value:
                raise ValueError("Current cached package source changed: " + member)
        found[name] = {"path": str(context / name), **actual[name]}
    return found


def verify_approved_source(ready):
    proof = json.loads(Path(ready).read_text())
    for name, value in proof["source_sha256"].items():
        if digest(BACKEND.parent / name) != value:
            raise ValueError("Reviewed source changed: " + name)
    return proof


def prepare(args):
    approved = verify_approved_source(args.source_ready)
    cached = checked_cached_wheels(args.cache_receipt)
    context = prepare_context(args.context, dependency_tag=DEPENDENCY_TAG)
    shutil.copyfile(Path(__file__).with_name("c09_stock_linux_fixture.py"), context / "c09_stock_linux_fixture.py")
    dockerfile = context / "Dockerfile"
    dockerfile.write_text(dockerfile.read_text() + "\nCOPY c09_stock_linux_fixture.py /usr/local/lib/python3.12/site-packages/fleet/\n")
    bindings = {"model-1": {"provider_use": "fleet.c09_stock_linux_fixture:BarrierModel", "target_model": "parent", "version": "v1"}}
    (context / "model-bindings.json").write_text(json.dumps(bindings, sort_keys=True))
    bundle = json.loads((context / "runtime-bundle.json").read_text())
    bundle.update(plugins=[], mcp_servers={}, secret_bindings={"operator-model-binding": bundle["secret_bindings"]["operator-model-binding"]})
    (context / "runtime-bundle.json").write_text(json.dumps(bundle, sort_keys=True))
    contracts = json.loads((context / "workspace-contracts.json").read_text())
    contracts.update(plugins=[], mcp_servers={})
    (context / "workspace-contracts.json").write_text(json.dumps(contracts, sort_keys=True))
    for name, value in cached.items():
        shutil.copyfile(value["path"], context / name)
    (context / "c09-cache-proof.json").write_text(
        json.dumps(
            {
                "receipt": str(args.cache_receipt.resolve()),
                "receipt_sha256": digest(args.cache_receipt),
                "dependency_image": DEPENDENCY,
                "cached_wheels": cached,
                "approved_source": approved["source_sha256"],
                "source_ready_sha256": digest(args.source_ready),
            },
            indent=2,
            sort_keys=True,
        )
    )
    seed = {str(p.relative_to(context)): digest(p) for p in sorted(context.rglob("*")) if p.is_file()}
    (context / "c09-prepared-inputs.json").write_text(json.dumps(seed, indent=2, sort_keys=True))
    return context


def build(args):
    if args.build_prepared:
        verify_approved_source(args.source_ready)
        context = args.context.resolve()
        seed = json.loads((context / "c09-prepared-inputs.json").read_text())
        if any(digest(context / name) != value for name, value in seed.items()):
            raise ValueError("Frozen prepared context changed")
    else:
        context = prepare(args)
    if args.prepare_only:
        return
    base_id = subprocess.check_output(["docker", "image", "inspect", "--format", "{{.Id}}", DEPENDENCY_TAG], text=True).strip()
    if base_id != DEPENDENCY:
        raise ValueError("Audited local dependency tag changed image identity")
    if not args.wheels_prepared:
        for options in ((), ("--package", "deerflow-harness"), ("--project", "packages/ecs-fleet")):
            subprocess.run(["uv", "build", "--wheel", "--offline", *options, "--out-dir", str(context)], cwd=BACKEND, check=True)
    inventory = wheel_inventory(context)
    if len(inventory) != 6:
        raise ValueError("C09 requires exactly three fresh and three verified cached wheels")
    roots = {"deer_flow": BACKEND, "deerflow_harness": BACKEND / "packages/harness", "deerflow_ecs_fleet": BACKEND / "packages/ecs-fleet", **CACHED_ROOTS}
    for name, wheel in inventory.items():
        root = next(root for prefix, root in roots.items() if name.startswith(prefix + "-"))
        for member, value in wheel["members"].items():
            if member.endswith(".py") and digest(root / member) != value:
                raise ValueError("Built wheel differs from current reviewed source: " + member)
    verify_approved_source(args.source_ready)
    (context / "wheel-inventory.json").write_text(json.dumps(inventory, indent=2, sort_keys=True))
    # Include fixture/builder bytes, actual libexecs, bindings/contracts and every
    # wheel member; no fabricated image id or recursive manifest hash.
    inputs = {str(p.relative_to(context)): digest(p) for p in sorted(context.rglob("*")) if p.is_file() and p.name != "completed-build-inputs.json"}
    (context / "completed-build-inputs.json").write_text(json.dumps({"files": inputs, "builder_sha256": digest(__file__), "fixture_sha256": digest(Path(__file__).with_name("c09_stock_linux_fixture.py"))}, indent=2, sort_keys=True))
    subprocess.run(["docker", "build", "--pull=false", "-t", args.tag, str(context)], cwd=BACKEND, check=True)
    subprocess.run(["docker", "image", "inspect", "--format", "{{.Id}}", args.tag], cwd=BACKEND, check=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--source-ready", type=Path, required=True)
    parser.add_argument("--cache-receipt", type=Path, required=True)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--build-prepared", action="store_true")
    parser.add_argument("--wheels-prepared", action="store_true")
    build(parser.parse_args())


if __name__ == "__main__":
    main()
