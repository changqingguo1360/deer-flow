"""Explicit installed-contract test data for native host cleanup fixtures.

This supplies no wheel/image proof. The original tests intentionally replace
installed bundle/provider discovery; their actual resource-cleanup behavior
continues through the original host with a real bounded contract file read.
"""

import json


def install_empty_workspace_contract(monkeypatch, tmp_path, private):
    import app.fleet.workspace_contracts as contracts

    assert not private.extensions.get_enabled_mcp_servers()
    assert not [item for item in private.plugins if item.enabled]
    bundle = {"skills": [], "plugins": [], "mcp_servers": {}, "secret_bindings": {}}
    payload = {
        "schema_version": 1,
        "host": {"use": contracts.HOST_USE, "sandbox_use": private.sandbox.use, "contract": contracts.HOST_CONTRACT},
        "plugins": [],
        "mcp_servers": {},
    }
    path = tmp_path / "installed-workspace-contracts.json"
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(contracts, "WORKSPACE_CONTRACT_PATH", str(path))
    return json.dumps(bundle).encode(), bundle
