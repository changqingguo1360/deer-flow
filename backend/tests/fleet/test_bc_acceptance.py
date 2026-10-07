"""BC aggregate acceptance consumes original observations; it never seeds success."""

import hashlib
import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree

REQUIRED_CASES = {
    "main": "test_bc10_installed_two_stock_workers_main",
    "boundary": "test_bc10_partition_cancel_restart_boundary",
}


def read_required(directory, name):
    path = directory / name
    if not path.is_file():
        raise ValueError("Missing required BC10 evidence: " + name)
    return json.loads(path.read_text())


def moment(value):
    if not value:
        raise ValueError("Missing actual execution timestamp")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def collect_main(directory):
    raw = read_required(directory, "main-raw.json")
    tables, processes = raw["tables"], raw["execution_containers"]
    attempts = tables["fleet_attempts"]
    agents = sorted((row for row in attempts if row["kind"] == "agent"), key=lambda row: moment(row["start_authorized_at"]))
    jobs = tables["fleet_jobs"]
    job_attempts = [row for row in attempts if row["kind"] == "job"]
    placements = {row["run_id"]: row for row in tables["fleet_run_placements"]}
    runs = {row["run_id"]: row for row in tables["runs"]}
    if len(agents) != 2 or len(jobs) != 1 or len(job_attempts) != 1:
        raise ValueError("Main requires the original two C attempts and one B attempt")
    task = tables["fleet_agent_tasks"]
    if len(task) != 1 or task[0]["state"] != "succeeded":
        raise ValueError("The original goal did not finish")
    for attempt in attempts:
        active = raw["running_container_observations"].get(attempt["id"])
        if not active or active["container"]["State"]["Pid"] <= 0 or active["attempt"]["node_session_id"] != attempt["node_session_id"]:
            raise ValueError("Actual original running PID/container/session evidence missing")
        observation = processes.get(attempt["id"])
        if not observation or not observation["Id"] or observation["Id"] != active["container"]["Id"] or observation["State"]["Running"] or not observation["State"]["FinishedAt"]:
            raise ValueError("Original physical STOP evidence missing")
        if observation["Image"] != raw["agent_image_id"]:
            raise ValueError("Original attempt did not use the freshly qualified execution image")
        if not attempt["node_session_id"] or not attempt["process_ref"] or not attempt["stopped_at"]:
            raise ValueError("Original execution identity or durable STOP missing")
    journals = {row["original_record_redacted"]["claim"]["attempt_id"]: row for row in raw["original_journals"]}
    if set(journals) != {row["id"] for row in attempts}:
        raise ValueError("Original worker journals missing")
    for attempt in attempts:
        journal = journals[attempt["id"]]
        record = journal["original_record_redacted"]
        if journal["mode"] != "0o600" or not journal["raw_sha256"] or record["claim"].get("token") or not record["claim"].get("token_sha256") or not record.get("reported"):
            raise ValueError("Original private journal/STOP receipt missing")
    if len(raw["worker_containers"]) != 2 or len({row["Id"] for row in raw["worker_containers"]}) != 2:
        raise ValueError("Two original installed stock workers required")
    for worker in raw["worker_containers"]:
        if worker["State"]["Pid"] <= 0 or worker["Config"]["Entrypoint"] != ["python", "-m", "deerflow_ecs_fleet.worker"]:
            raise ValueError("Actual original stock worker identity missing")
    group = tables["fleet_wait_groups"]
    links = tables["fleet_job_links"]
    if len(group) != 1 or group[0]["state"] != "dispatched" or group[0]["parent_run_id"] != agents[0]["run_id"] or group[0]["continuation_run_id"] != agents[1]["run_id"] or group[0]["job_ids"] != [jobs[0]["id"]]:
        raise ValueError("Original wait-group C/B/C lineage missing")
    if len(links) != 1 or links[0]["link_mode"] != "awaited" or links[0]["job_id"] != jobs[0]["id"] or links[0]["agent_task_id"] != task[0]["id"]:
        raise ValueError("Original awaited child link missing")
    points = {row["id"]: row for row in tables["fleet_workspace_points"]}
    checkpoint_ids = {row["checkpoint_id"] for row in tables["checkpoints"]}
    event_ids = [row["id"] for row in tables["run_events"]]
    if not checkpoint_ids or not event_ids or len(event_ids) != len(set(event_ids)):
        raise ValueError("Actual checkpoint/run-event evidence absent or duplicate")
    for attempt in agents:
        run, placement = runs[attempt["run_id"]], placements[attempt["run_id"]]
        point = points.get(placement["final_workspace_point_id"])
        if placement["agent_task_id"] != task[0]["id"] or placement["active_attempt_id"] != attempt["id"]:
            raise ValueError("Original task/run/attempt lineage mismatch")
        if run["owner_worker_id"] != "fleet-agent:" + attempt["id"] or not run["lease_expires_at"]:
            raise ValueError("Actual SQL writer ownership/lease evidence missing")
        if not point or point["attempt_id"] != attempt["id"] or point["checkpoint_id"] not in checkpoint_ids:
            raise ValueError("Original accepted checkpoint/workspace pair missing")
        if not any(row["run_id"] == attempt["run_id"] for row in tables["run_events"]):
            raise ValueError("Actual writer run-event identifiers missing")
    overlaps = []
    for index, left in enumerate(agents):
        for right in agents[index + 1 :]:
            # Both server-authorized intervals and actual container execution
            # intervals must be disjoint for this same original thread.
            sql_overlap = max(moment(left["start_authorized_at"]), moment(right["start_authorized_at"])) < min(moment(left["stopped_at"]), moment(right["stopped_at"]))
            a, b = processes[left["id"]]["State"], processes[right["id"]]["State"]
            physical_overlap = max(moment(a["StartedAt"]), moment(b["StartedAt"])) < min(moment(a["FinishedAt"]), moment(b["FinishedAt"]))
            if sql_overlap or physical_overlap:
                overlaps.append([left["id"], right["id"]])
    # Inventory every B attempt's output, including never accepted outcomes.
    output_rows = raw["all_job_attempt_outputs"]
    if {row["attempt_id"] for row in output_rows} != {row["id"] for row in job_attempts}:
        raise ValueError("Missing accepted, rejected or uncertain B attempt output")
    effects = []
    for row in output_rows:
        if not row["output_scanned"]:
            raise ValueError("Original attempt output was not scanned")
        for line in row["effect_lines"]:
            effect = json.loads(line)
            if effect["logical_effect"] != "bc10-original-child-result" or effect["pid"] <= 0:
                raise ValueError("Invalid original child effect identity")
            effects.append({"job_id": row["job_id"], "attempt_id": row["attempt_id"], **effect})
    if {row["pid"] for row in raw["child_http_receipts"]} != {row["pid"] for row in effects}:
        raise ValueError("Actual child PID receipt does not match original effects")
    counts = Counter((effect["job_id"], effect["logical_effect"]) for effect in effects)
    duplicate_effects = sum(max(0, count - 1) for count in counts.values())
    if len(effects) != len(jobs):
        raise ValueError("Original child effect missing")
    manifests = {row["id"]: row for row in tables["fleet_artifact_manifests"]}
    manifest = manifests.get(jobs[0]["accepted_manifest_id"])
    if not manifest or manifest["attempt_id"] != job_attempts[0]["id"]:
        raise ValueError("Original B accepted manifest missing")
    observed = raw["provider_calls"][-1]["observed_original_results"]
    if len(observed) != 1 or observed[0]["job_id"] != jobs[0]["id"] or observed[0]["manifest_id"] != manifest["id"] or observed[0]["state"] != jobs[0]["state"]:
        raise ValueError("Original continuation did not receive the original accepted result identity")
    expected_files = {item["path"]: {key: item[key] for key in ("path", "size", "sha256")} for item in manifest["files"]}
    if {item["path"]: item for item in observed[0]["files"]} != expected_files:
        raise ValueError("Original continuation result files differ from the original accepted manifest")
    reservations = tables["fleet_reservations"]
    if {row["attempt_id"] for row in reservations} != {row["id"] for row in attempts}:
        raise ValueError("Original execution ledger is incomplete")
    leaks = [row["id"] for row in reservations if row["state"] != "released" or row["released_at"] is None]
    child = job_attempts[0]
    across = (
        agents[0]["node_id"] != agents[1]["node_id"]
        and jobs[0]["source_run_id"] == agents[0]["run_id"]
        and moment(agents[0]["stopped_at"]) <= moment(child["start_authorized_at"])
        and moment(child["stopped_at"]) <= moment(agents[1]["start_authorized_at"])
    )
    browser = read_required(directory, "browser-observations.json")
    if browser["thread_id"] != raw["thread_id"] or browser["task_id"] != task[0]["id"] or browser["job_id"] != jobs[0]["id"] or browser["accepted_manifest_id"] != jobs[0]["accepted_manifest_id"]:
        raise ValueError("Browser did not observe the same original flow")
    for state in ("waiting", "completed"):
        if not (directory / ("browser-" + state + ".png")).is_file():
            raise ValueError("Actual browser capture missing")
    if browser["waiting"]["state"] != "waiting_jobs" or browser["completed"]["state"] != "succeeded" or browser["completed"]["stop_state"] != "confirmed" or browser["completed"]["resources_held"]:
        raise ValueError("Browser goal/run/STOP evidence incomplete")
    source = read_required(directory, "installed-source-audit.json")
    if not source["agent"]["verified_members"] or not source["worker"]["verified_members"] or source["agent"]["image_id"] != raw["agent_image_id"] or source["worker"]["image_id"] != raw["worker_image_id"]:
        raise ValueError("Fresh installed source qualification missing")
    return {
        "scope": "fresh installed BC10 main only; full release requires boundary and carried B/C",
        "c_b_c_across_nodes": across,
        "duplicate_effects": duplicate_effects,
        "thread_double_writes": len(overlaps),
        "capacity_leaks": len(leaks),
        "writer_overlaps": overlaps,
        "effect_records": effects,
        "task_id": task[0]["id"],
        "run_ids": [row["run_id"] for row in agents],
        "job_ids": [row["id"] for row in jobs],
        "attempt_ids": [row["id"] for row in attempts],
        "checkpoint_ids": sorted(checkpoint_ids),
        "run_event_ids": event_ids,
    }


def validate_selection(report, *, selected):
    root = ElementTree.parse(report).getroot()
    cases = root.findall(".//testcase")
    names = {case.attrib["name"] for case in cases}
    required = {REQUIRED_CASES[phase] for phase in selected}
    if not required <= names:
        raise ValueError("Required selected BC10 cases missing")
    for case in cases:
        if case.attrib["name"] in required and any(case.find(tag) is not None for tag in ("skipped", "failure", "error")):
            raise ValueError("Required selected BC10 case skipped or failed")
    return {"selected": sorted(selected), "observed": sorted(names), "unselected_requirements": sorted(set(REQUIRED_CASES) - set(selected))}


def test_bc10_main_evidence_collector():
    evidence = Path(os.environ["BC10_EVIDENCE_DIR"])
    collected = collect_main(evidence)
    assert collected["c_b_c_across_nodes"]
    assert collected["duplicate_effects"] == collected["thread_double_writes"] == collected["capacity_leaks"] == 0
    (evidence / "main-derived.json").write_text(json.dumps(collected, indent=2))


def test_bc_combined_release_aggregate():
    evidence = Path(os.environ["BC10_EVIDENCE_DIR"])
    collected = collect_main(evidence)
    assert collected["c_b_c_across_nodes"]
    assert collected["duplicate_effects"] == collected["thread_double_writes"] == collected["capacity_leaks"] == 0
    validate_selection(evidence / "main.xml", selected={"main"})
    validate_selection(evidence / "boundary.xml", selected={"boundary"})
    boundary = read_required(evidence, "boundary-derived.json")
    assert boundary["partition_observed"] and boundary["authenticated_cancel_observed"] and boundary["gateway_restart_observed"] and boundary["stock_worker_restart_observed"]
    assert boundary["resolve_without_stop_status"] == 409 and boundary["resolve_after_original_stop_status"] == 200
    assert boundary["duplicate_effects"] == boundary["thread_double_writes"] == boundary["capacity_leaks"] == 0
    carried = read_required(evidence, "carried-b-c.json")
    assert set(carried) == {"B", "C"}
    for label, proof in carried.items():
        assert proof["scope"] == "carried; not fresh BC10 execution", label
        assert proof["required_cases"] and proof["source_qualification"], label
        for receipt in proof["receipts"]:
            path = Path(receipt["path"])
            assert path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == receipt["sha256"], label
        assert proof["passed"] == proof["required_cases"] and proof["skipped"] == 0, label
