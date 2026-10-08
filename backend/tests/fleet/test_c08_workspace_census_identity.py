"""Containment accepts scheduling changes and rejects physical identity drift."""

import copy
import json
from pathlib import Path

import pytest
from deerflow_ecs_fleet.worker.workspace_publication import AgentWorkspacePublication


def captured_observations():
    # Actual Linux stable observation fields only: no claim nonce, token or
    # private request identity. The full ignored raw evidence is hash-anchored.
    return json.loads((Path(__file__).parent / "fixtures/c08-census-scheduler-state.json").read_text())["observations"]


@pytest.mark.parametrize("reverse", (False, True))
def test_original_runner_scheduling_transition_does_not_replace_containment(reverse):
    before, after = captured_observations()
    AgentWorkspacePublication._same_container(*(after, before) if reverse else (before, after))


@pytest.mark.parametrize("state", ("Z", "X", "x", "?", "", None))
def test_containment_never_accepts_same_dead_or_unverifiable_runner_state(state):
    before, after = captured_observations()
    before["receipt"]["runner"]["state"] = after["receipt"]["runner"]["state"] = state
    with pytest.raises(ValueError):
        AgentWorkspacePublication._same_container(before, after)


@pytest.mark.parametrize("field", ("pid", "ppid", "start_ticks", "uid", "state"))
def test_containment_missing_original_runner_observation_fails_closed(field):
    before, after = captured_observations()
    after = copy.deepcopy(before)
    del before["receipt"]["runner"][field]
    del after["receipt"]["runner"][field]
    with pytest.raises(ValueError):
        AgentWorkspacePublication._same_container(before, after)


@pytest.mark.parametrize("field", ("pid", "ppid", "start_ticks", "uid", "container_id", "started_at", "image", "launch_fingerprint", "pid_namespace", "cgroup_digest"))
def test_containment_rejects_original_stable_identity_drift(field):
    before, after = captured_observations()
    after = copy.deepcopy(before)
    if field in ("pid", "ppid", "start_ticks", "uid"):
        after["receipt"]["runner"][field] += 1
    elif field == "pid_namespace":
        after["receipt"]["collector"][field] += 1
    elif field == "cgroup_digest":
        after["receipt"][field] = "0" * 64
    else:
        after[field] = "another-original-value"
    with pytest.raises(ValueError):
        AgentWorkspacePublication._same_container(before, after)
