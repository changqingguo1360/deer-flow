"""Public worker process boundary, without secrets on argv or in diagnostics."""

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Fleet worker/operator require a POSIX node host")

PACKAGE = Path(__file__).resolve().parents[2] / "packages" / "ecs-fleet"


async def worker(*args):
    proc = await asyncio.create_subprocess_exec(sys.executable, "-m", "deerflow_ecs_fleet.worker", *args, env=os.environ | {"PYTHONPATH": str(PACKAGE)}, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await asyncio.wait_for(proc.communicate(), 5)
    return proc.returncode, (out + err).decode()


@pytest.mark.asyncio
async def test_public_entry_documents_file_only_credential():
    code, output = await worker("--help")
    assert code == 0, output
    assert "--settings" in output
    assert "--credential " not in output


@pytest.mark.asyncio
async def test_private_credential_failure_does_not_disclose_secret(tmp_path):
    token = "df_fleet_" + "super-secret-test" * 3
    credential = tmp_path / "credential"
    credential.write_text(token)
    credential.chmod(0o644)
    settings = tmp_path / "worker.json"
    settings.write_text(json.dumps({"gateway_url": "http://127.0.0.1:1", "credential_file": str(credential), "state_dir": str(tmp_path / "state"), "nas_root": str(tmp_path / "nas"), "nas_identity": "test"}))
    code, output = await worker("--settings", str(settings))
    assert code == 1
    assert "Worker startup or recovery failed" in output
    assert token not in output
    assert not (tmp_path / "state").exists()


@pytest.mark.asyncio
async def test_operator_register_issue_query_drain_revoke(fleet_database, tmp_path):
    from deerflow_ecs_fleet.config import FleetConfig
    from deerflow_extension_api import ExtensionRuntimeDeps

    from .test_b02_fleet_foundation import service_class

    _, sf, _ = fleet_database
    service = service_class()(FleetConfig(enabled=True))
    await service.start(ExtensionRuntimeDeps(session_factory=sf))
    try:
        assert hasattr(service.nodes, "register"), "Trusted operator cannot register a node without handwritten SQL"
        await service.nodes.register(node_id="b11", name="worker-b11", cpu_millis=1000, memory_mib=512)
        credential = await service.credentials.issue("b11", lifetime_seconds=60)
        assert (await service.nodes.status("b11"))["node"]["admin_state"] == "enabled"
        await service.nodes.set_admin_state("b11", "draining")
        assert (await service.nodes.status("b11"))["node"]["admin_state"] == "draining"
        await service.credentials.revoke(credential.credential_id)
        with pytest.raises(PermissionError):
            await service.credentials.authenticate("Bearer " + credential.token)
        await service.nodes.set_admin_state("b11", "disabled")
        status = await service.nodes.status("b11")
        assert status["node"]["admin_state"] == "disabled"
        assert credential.token not in json.dumps(status)
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_public_operator_cli_writes_private_credential(fleet_database, tmp_path):
    _, _, schema = fleet_database
    settings = tmp_path / "operator.json"
    settings.write_text(json.dumps({"database_url": os.environ["TEST_POSTGRES_URI"], "schema": schema, "fleet": {"enabled": True}}))
    settings.chmod(0o600)

    async def command(*args):
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "deerflow_ecs_fleet.operator", "--settings", str(settings), *args, env=os.environ | {"PYTHONPATH": str(PACKAGE)}, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, err = await asyncio.wait_for(proc.communicate(), 10)
        return proc.returncode, (out + err).decode()

    code, output = await command("register", "--node-id", "operator-test", "--name", "worker-test", "--cpu-millis", "1000", "--memory-mib", "512")
    assert code == 0, output
    credential = tmp_path / "credential"
    code, output = await command("issue", "--node-id", "operator-test", "--credential-file", str(credential), "--lifetime-seconds", "60")
    assert code == 0, output
    assert stat_mode(credential) == 0o600
    token = credential.read_text().strip()
    assert token.startswith("df_fleet_") and token not in output
    credential_id = json.loads(output)["credential_id"]
    for action in ("drain", "status", "disable", "enable"):
        code, output = await command(action, "--node-id", "operator-test")
        assert code == 0, output
        assert token not in output
    code, output = await command("revoke", "--credential-id", credential_id)
    assert code == 0, output
    code, output = await command("issue", "--node-id", "operator-test", "--credential-file", str(credential), "--lifetime-seconds", "60")
    assert code == 1 and token not in output
    assert credential.read_text().strip() == token


def stat_mode(path):
    import stat

    return stat.S_IMODE(path.stat().st_mode)


@pytest.mark.asyncio
@pytest.mark.parametrize("record", [{"reported": False}, {"reported": True, "server_state": "running", "stop_reason": "exit", "exit_code": 0}])
async def test_entry_reports_failed_shutdown_stop_proof(tmp_path, monkeypatch, record):
    from deerflow_ecs_fleet.worker import __main__ as entry
    from deerflow_ecs_fleet.worker.journal import AttemptJournal

    nas = tmp_path / "nas"
    nas.mkdir()
    (nas / ".deerflow-fleet-root").write_text("test\n")
    secret = tmp_path / "credential"
    secret.write_text("df_fleet_test")
    secret.chmod(0o600)
    state = tmp_path / "state"

    # Daemon stop proof was unavailable; this durable evidence must prevent a
    # successful process exit even when shutdown gathers child exceptions.
    class LostStopDaemon:
        def __init__(self, **kwargs):
            self.client = kwargs["client"]
            self.client.node_id = "test"

        async def run(self, **kwargs):
            AttemptJournal(state).save({"node_id": "test", "claim": {"attempt_id": "test-attempt"}} | record)

    monkeypatch.setattr(entry, "NodeDaemon", LostStopDaemon)
    with pytest.raises(RuntimeError, match="shutdown"):
        await entry.run_worker(entry.WorkerSettings(gateway_url="http://127.0.0.1:1", credential_file=secret, state_dir=state, nas_root=nas, nas_identity="test"))


@pytest.mark.parametrize(
    "field,value",
    [("max_parallel", 0), ("max_parallel", 65), ("poll_seconds", 0), ("timeout_seconds", 61), ("renew_seconds", 31), ("safety_margin_seconds", 16), ("state_dir", "relative"), ("nas_identity", "../other"), ("unexpected", True)],
)
def test_settings_reject_unbounded_or_unknown_values(tmp_path, field, value):
    from deerflow_ecs_fleet.worker.__main__ import WorkerSettings
    from pydantic import ValidationError

    values = {"gateway_url": "http://127.0.0.1:1", "credential_file": tmp_path / "credential", "state_dir": tmp_path / "private", "nas_root": tmp_path / "nas", "nas_identity": "test"} | {field: value}
    with pytest.raises(ValidationError):
        WorkerSettings.model_validate(values)


def test_private_files_reject_symlinks_and_nas_overlap(tmp_path):
    from deerflow_ecs_fleet.worker.__main__ import WorkerSettings, read_file
    from pydantic import ValidationError

    secret = tmp_path / "secret"
    secret.write_text("df_fleet_test")
    secret.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(secret)
    with pytest.raises(OSError):
        read_file(link, private=True)
    with pytest.raises(ValidationError):
        WorkerSettings(gateway_url="http://127.0.0.1:1", credential_file=secret, state_dir=tmp_path / "nas" / "private", nas_root=tmp_path / "nas", nas_identity="test")
