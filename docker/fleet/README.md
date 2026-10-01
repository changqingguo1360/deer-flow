# Fleet worker runtime

B11 host worker entry and deployment helpers have local acceptance evidence. The B12
release gate and remote Agent runner remain pending; all Fleet flags default to false.

The worker is an outbound-only trusted node daemon. It controls host Docker; job
containers never receive the Docker socket, node credentials or private daemon state.
No worker HTTP service or published port is needed.

Worker entry:

```bash
python -m deerflow_ecs_fleet.worker --settings /absolute/path/worker.json
```

Example Compose worker settings (credential/state use container targets; NAS keeps its host path):

```json
{
  "gateway_url": "https://gateway.example.invalid",
  "credential_file": "/run/fleet/credential",
  "state_dir": "/var/lib/deerflow-fleet",
  "nas_root": "/srv/deerflow-fleet-nas",
  "nas_identity": "fleet-production",
  "max_parallel": 1,
  "timeout_seconds": 10,
  "renew_seconds": 10,
  "safety_margin_seconds": 5,
  "poll_seconds": 0.25
}
```

Keep credentials in a private, owned regular file, mounted read-only into the daemon.
Do not place their values in settings, command arguments, environment variables or
repository files. Keep state outside NAS with private permissions and preserve it
across restarts. NAS's explicit `.deerflow-fleet-root` sentinel must contain the configured
identity plus a newline. A missing or mismatched root is an error.

The reproducible image build uses operator-provided digest-pinned base/runtime inputs,
a verified real Linux Docker CLI, and a hashed offline wheelhouse. The worker image
must include the real entry and its runtime dependencies; a shell placeholder cannot
verify deployment. Publish the built worker image by digest after independent validation.
Production images/credentials are not supplied by local acceptance fixtures.

Compose mounts NAS at the same absolute path visible to the host Docker daemon and
keeps each node's state and credential separate. Validate Compose before starting;
workers publish no ports. Run only one daemon incarnation per node credential.

For registration, credential lifecycle, drain/disable, compatibility checks and the
accepted-work upgrade/rollback order, see the [deployment guide](../../docs/deployment/ecs-fleet.md).
Local validation scope will be recorded there after the real CLI/build tests finish.

## Frozen offline build

Prepare an operator-approved artifact directory; do not put credentials or runtime
settings in it:

```text
fleet-artifacts/
├── docker.tgz          # verified real static Linux Docker CLI archive
├── requirements.lock   # every runtime dependency pinned with wheel hashes
├── SHA256SUMS          # artifact inventory and checksums, including lock
├── provenance.json     # source/architecture/version notes, retained by operator
└── wheelhouse/         # exact compatible binary wheels
```

The committed `runtime-cp314-linux-aarch64.lock` records the local CPython 3.14/Linux
aarch64 acceptance artifact set. It is a reference for that platform, not a default
for another Python/CPU combination. Other platforms require an independently reviewed
matching base, Docker CLI and hashed wheel lock. Freeze and retain artifacts; resolving
new dependency versions on each rebuild is not reproducible.

For the local reference set, run from repo root with a pip-equipped Python environment:

```bash
export FLEET_BUILD_ARTIFACTS=/absolute/fleet-artifacts
export FLEET_WORKER_BASE=python@sha256:51dafde81dbdb6ebde285137a295cf18a47ca95234fe388a343719cb97305b3d
export FLEET_DOCKER_CLI_SHA256=e1590e656abaf2dfe8a1d724d99b82644446fe24844f438ea000e08006774717
mkdir -p "$FLEET_BUILD_ARTIFACTS/wheelhouse"
cp docker/fleet/runtime-cp314-linux-aarch64.lock "$FLEET_BUILD_ARTIFACTS/requirements.lock"
python3 -m pip download --index-url https://pypi.org/simple --only-binary=:all: --require-hashes --python-version 3.14 --implementation cp --abi cp314 --platform manylinux_2_17_aarch64 --platform manylinux2014_aarch64 --dest "$FLEET_BUILD_ARTIFACTS/wheelhouse" -r "$FLEET_BUILD_ARTIFACTS/requirements.lock"
curl --fail --location --proto '=https' https://download.docker.com/linux/static/stable/aarch64/docker-29.2.0.tgz -o "$FLEET_BUILD_ARTIFACTS/docker.tgz"
```

Create the checksum inventory after verifying the frozen CLI checksum. The following
uses Python so it also works on local POSIX hosts without the Linux `sha256sum` command:

```bash
python3 - "$FLEET_BUILD_ARTIFACTS" "$FLEET_DOCKER_CLI_SHA256" <<'PYCODE'
import hashlib, json, sys
from pathlib import Path
artifact_root = Path(sys.argv[1])
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
if digest(artifact_root / "docker.tgz") != sys.argv[2]:
    raise SystemExit("Docker CLI checksum mismatch")
(artifact_root / "provenance.json").write_text(json.dumps({
    "platform": "linux/aarch64", "python": "3.14",
    "docker_cli_url": "https://download.docker.com/linux/static/stable/aarch64/docker-29.2.0.tgz",
    "docker_cli_sha256": sys.argv[2], "wheel_source": "https://pypi.org/simple"
}, indent=2) + "\n")
files = sorted(path for path in artifact_root.rglob("*") if path.is_file() and path.name != "SHA256SUMS")
(artifact_root / "SHA256SUMS").write_text("".join(
    f"{digest(path)}  {path.relative_to(artifact_root).as_posix()}\n" for path in files
))
PYCODE
```

Build without network access or automatic image pulls. Preload/verify the frozen base
through the operator's artifact process before running this command:

```bash
docker build --pull=false --network=none --build-arg WORKER_BASE="$FLEET_WORKER_BASE" --build-arg DOCKER_CLI_SHA256="$FLEET_DOCKER_CLI_SHA256" --build-context fleet_artifacts="$FLEET_BUILD_ARTIFACTS" --iidfile /tmp/fleet-worker-image-id -t fleet-worker:local-verified -f docker/fleet/worker.Dockerfile .
docker image inspect fleet-worker:local-verified --format '{{.Id}}'
docker run --rm --network none fleet-worker:local-verified --help
```

Compare the inspected content ID with the iidfile. The whitelist build context copies
only the exact Fleet source package; runtime dependencies install offline with hashes.
The resulting image runs only the worker entry. Operator commands run in the trusted
host environment with the full extension dependencies. After release validation, use
an operator-provided registry digest or a verified local content ID for deployment.

## Compose inputs

Provision existing private state/credential/settings paths first. The bind mounts refuse
missing host paths; Compose does not create them. On Linux the default daemon UID is
root: credential/state ownership must match that UID. Gateway and workers must also
use compatible NAS filesystem identities; private 0700 workspaces are not shared
between unrelated UIDs. The standard root Gateway/worker images share UID 0. A host CLI worker instead uses
its current owner's private paths. NAS remains mounted at its identical absolute host
path. Example non-secret environment (replace the digest and host paths):

```bash
export FLEET_WORKER_IMAGE=registry.example.invalid/fleet-worker@sha256:REPLACE_WITH_VERIFIED_DIGEST
export FLEET_WORKER_SETTINGS=/etc/deerflow-fleet/worker-a.json
export FLEET_WORKER_CREDENTIAL=/run/deerflow-fleet/worker-a-credential
export FLEET_WORKER_STATE=/var/lib/deerflow-fleet/worker-a
export FLEET_NAS_ROOT=/srv/deerflow-fleet-nas
export FLEET_DOCKER_SOCKET=/var/run/docker.sock
docker compose --profile fleet-worker -f docker/fleet/compose.yaml config
docker compose --profile fleet-worker -f docker/fleet/compose.yaml up --no-build -d worker
```

The base, CLI hash and artifact directory variables above are also required when
rendering this Compose file. The settings file uses `/run/fleet/credential` and
`/var/lib/deerflow-fleet` as its credential/state targets. Start a second worker on its
own Docker host with a distinct credential and persistent state. There is no published
worker port. Automatic restart is disabled: an unresolved recovery exits nonzero and
requires operator review; retain the journal and prove stop before resuming admission.

Local evidence verifies two actual host CLI workers and image entry/Linux Docker
control, plus rendered Compose mounts/ports. It does not claim a complete containerized
Compose daemon deployment or production ECS/NAS deployment. B12 remains the release gate.
