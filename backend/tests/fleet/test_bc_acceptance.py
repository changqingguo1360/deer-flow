"""BC aggregate acceptance consumes original observations; it never seeds success."""

import ast
import hashlib
import json
import os
import re
import subprocess
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
    if (
        source["agent"]["mismatches"]
        or source["worker"]["mismatches"]
        or not source["agent"]["verified_members"]
        or not source["worker"]["verified_members"]
        or source["agent"]["image_id"] != raw["agent_image_id"]
        or source["worker"]["image_id"] != raw["worker_image_id"]
    ):
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


def collect_boundary(directory):
    observations = read_required(directory, "boundary-observations.json")
    phases = {name: read_required(directory, "boundary-" + name + ".json") for name in observations["phase_names"]}
    before = phases["before-partition"]
    unknown = phases["quarantined-without-durable-stop"]
    stopped = phases["original-stop-replayed"]
    resolved = phases["manual-fail-stopped"]
    cancelled = phases["cancelled-before-gateway-restart"]
    final = phases["cancel-replayed-after-gateway-restart"]
    actions = {row["action"]: row for row in observations["actions"] if row["action"] != "authorized-drain"}
    attempts = final["tables"]["fleet_attempts"]
    agents = [row for row in attempts if row["kind"] == "agent"]
    children = [row for row in attempts if row["kind"] == "job"]
    if len(agents) != 1 or len(children) != 1 or {row["id"] for row in attempts} != {row["id"] for row in before["tables"]["fleet_attempts"]}:
        raise ValueError("Boundary executed a duplicate or unexpected continuation")
    agent, child = agents[0], children[0]
    job_id, attempt_id = child["job_id"], child["id"]

    def row(phase, table, key, value):
        rows = [item for item in phase["tables"][table] if item[key] == value]
        if len(rows) != 1:
            raise ValueError("Required unique original boundary row missing")
        return rows[0]

    u = row(unknown, "fleet_attempts", "id", attempt_id)
    ur = row(unknown, "fleet_reservations", "attempt_id", attempt_id)
    if u["state"] != "unknown" or u["stopped_at"] is not None or ur["state"] != "quarantined" or moment(unknown["database_clock"]) < moment(u["lease_expires_at"]):
        raise ValueError("Real lease-expired quarantine without durable STOP absent")
    if actions["resolve-without-durable-stop"]["status"] != 409:
        raise ValueError("Original no-STOP recovery was not refused")
    for table in ("fleet_jobs", "fleet_attempts", "fleet_reservations", "fleet_recovery_events"):
        if unknown["tables"][table] != phases["after-refused-resolve"]["tables"][table]:
            raise ValueError("Refused original recovery changed execution facts")
    for phase in (before, final):
        for attempt in phase["tables"]["fleet_attempts"]:
            running = phase["running_container_observations"].get(attempt["id"])
            physical = phase["execution_containers"].get(attempt["id"])
            if not running or running["container"]["State"]["Pid"] <= 0 or not physical or physical["Id"] != running["container"]["Id"] or physical["Image"] != phase["agent_image_id"]:
                raise ValueError("Actual original execution PID/container/image evidence absent")
    sr = row(stopped, "fleet_reservations", "attempt_id", attempt_id)
    sa = row(stopped, "fleet_attempts", "id", attempt_id)
    if not sa["stopped_at"] or sr["state"] != "released" or not sr["released_at"] or stopped["execution_containers"][attempt_id]["State"]["Running"]:
        raise ValueError("Original journal replay did not prove physical STOP/release")
    node_id = child["node_id"]
    old_node, new_node = row(before, "fleet_nodes", "id", node_id), row(stopped, "fleet_nodes", "id", node_id)
    restart = actions["stock-journal-restart"]
    if old_node["session_id"] == new_node["session_id"] or not restart["observations"] or restart["cli_pid"] <= 0:
        raise ValueError("Stock Worker restart/new session was not observed")
    old_worker = before["worker_containers"][1]
    for observed in restart["observations"]:
        container = observed["container"]
        if container["Id"] != old_worker["Id"] or container["Image"] != before["worker_image_id"] or container["State"]["Pid"] <= 0 or container["Config"]["Entrypoint"] != ["python", "-m", "deerflow_ecs_fleet.worker"]:
            raise ValueError("Restart substituted the original installed Worker")
    journals = {item["original_record_redacted"]["claim"]["attempt_id"]: item["original_record_redacted"] for item in stopped["original_journals"]}
    if not journals[attempt_id].get("reported") or journals[attempt_id].get("server_state") != "unknown":
        raise ValueError("Original uncertain STOP journal replay missing")
    job = row(resolved, "fleet_jobs", "id", job_id)
    events = resolved["tables"]["fleet_recovery_events"]
    if actions["resolve-after-original-stop"]["status"] != 200 or job["state"] != "failed" or job["accepted_manifest_id"] is not None or len(events) != 1 or events[0]["action"] != "fail_stopped" or events[0]["attempt_id"] != attempt_id:
        raise ValueError("Manual resolution must preserve failed uncertain outcome")
    cancel = actions["authenticated-parent-cancel"]
    replay = actions["authenticated-cancel-replay-after-gateway-restart"]
    if cancel["body"] != replay["body"] or cancel["result"] != replay["result"] or cancel["result"]["state"] != "cancelled":
        raise ValueError("Authenticated cancellation identity did not survive Gateway restart")
    for table in ("fleet_agent_tasks", "fleet_task_budgets", "fleet_task_operation_receipts", "fleet_jobs", "fleet_attempts", "fleet_reservations", "fleet_recovery_events", "checkpoints", "checkpoint_writes", "run_events"):
        if cancelled["tables"][table] != final["tables"][table]:
            raise ValueError("Gateway cancellation replay duplicated durable mutation")
    task = final["tables"]["fleet_agent_tasks"][0]
    if task["state"] != "cancelled" or len(final["tables"]["fleet_task_operation_receipts"]) != 1:
        raise ValueError("Original goal cancellation did not settle")
    if not agent["stopped_at"] or not agent["finished_at"]:
        raise ValueError("Accepted original C STOP is missing")
    placements = {item["run_id"]: item for item in final["tables"]["fleet_run_placements"]}
    points = {item["id"]: item for item in final["tables"]["fleet_workspace_points"]}
    point = points.get(placements[agent["run_id"]]["final_workspace_point_id"])
    run = row(final, "runs", "run_id", agent["run_id"])
    checkpoint_ids = {item["checkpoint_id"] for item in final["tables"]["checkpoints"]}
    if (
        not point
        or point["checkpoint_id"] not in checkpoint_ids
        or point["attempt_id"] != agent["id"]
        or run["owner_worker_id"] != "fleet-agent:" + agent["id"]
        or not run["lease_expires_at"]
        or not any(item["run_id"] == agent["run_id"] for item in final["tables"]["run_events"])
    ):
        raise ValueError("Actual C writer/checkpoint/lease/run-event evidence absent")
    overlaps = []
    for index, left in enumerate(agents):
        for right in agents[index + 1 :]:
            a, b = final["execution_containers"][left["id"]]["State"], final["execution_containers"][right["id"]]["State"]
            if max(moment(left["start_authorized_at"]), moment(right["start_authorized_at"])) < min(moment(left["stopped_at"]), moment(right["stopped_at"])) or max(moment(a["StartedAt"]), moment(b["StartedAt"])) < min(
                moment(a["FinishedAt"]), moment(b["FinishedAt"])
            ):
                overlaps.append([left["id"], right["id"]])
    if observations["configured_lease_seconds"] != 120:
        raise ValueError("Original configured safety budget changed")
    outputs = final["all_job_attempt_outputs"]
    if {item["attempt_id"] for item in outputs} != {attempt_id} or not all(item["output_scanned"] for item in outputs):
        raise ValueError("All original uncertain attempt outputs required")
    effects = [{"job_id": output["job_id"], "attempt_id": output["attempt_id"], **json.loads(line)} for output in outputs for line in output["effect_lines"]]
    if not effects or {item["pid"] for item in effects} != {item["pid"] for item in final["child_http_receipts"]}:
        raise ValueError("Original child effects/PID identity missing")
    if any(item["logical_effect"] != "bc10-original-child-result" or item["pid"] <= 0 for item in effects):
        raise ValueError("Original effect identity invalid")
    counts = Counter((item["job_id"], item["logical_effect"]) for item in effects)
    leaks = [item["id"] for item in final["tables"]["fleet_reservations"] if item["state"] != "released" or not item["released_at"]]
    for attempt in attempts:
        if final["execution_containers"][attempt["id"]]["State"]["Running"]:
            raise ValueError("Original execution physically live after cancellation")
    transport = observations["transport"]
    if not any(item["action"] == "disconnected" for item in transport) or not any(item["action"] == "connected" for item in transport[1:]):
        raise ValueError("Real control-plane partition/reconnection not observed")
    for name in ("worker-tls-lifecycle.json", "restarted-worker-tls-lifecycle.json"):
        lifecycle = read_required(directory, name)
        if not lifecycle["server_task_done"] or not lifecycle["socket_closed"]:
            raise ValueError("Original Gateway listener restart not settled")
    gateways = read_required(directory, "boundary-gateway-identities.json")
    if len(gateways) != 3 or len({item["app_identity"] for item in gateways}) != 3 or len({item["runtime_identity"] for item in gateways}) != 3 or len({item["run_manager_identity"] for item in gateways}) != 3:
        raise ValueError("Actual new Gateway app/runtime/manager identities missing")
    for gateway in gateways:
        if not gateway["ready"] or not gateway["http_url"] or not gateway["tls_url"]:
            raise ValueError("Original restarted listeners/services were not ready")
        for previous in gateway["previous_shutdown"]:
            if previous["ready"] or not previous["client_closed"] or previous["continuation_tasks"] or not previous["reconciler_cleared"]:
                raise ValueError("Previous Gateway runtime/lifespan was not closed")
    return {
        "scope": observations["scope"],
        "partition_observed": True,
        "authenticated_cancel_observed": True,
        "gateway_restart_observed": True,
        "stock_worker_restart_observed": True,
        "resolve_without_stop_status": actions["resolve-without-durable-stop"]["status"],
        "resolve_after_original_stop_status": actions["resolve-after-original-stop"]["status"],
        "duplicate_effects": sum(max(0, count - 1) for count in counts.values()),
        "thread_double_writes": len(overlaps),
        "capacity_leaks": len(leaks),
        "task_id": task["id"],
        "job_id": job_id,
        "attempt_ids": [item["id"] for item in attempts],
        "operation_id": cancel["result"]["operation_id"],
        "effect_records": effects,
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


def verified_reference(reference):
    path = Path(reference["path"])
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != reference["sha256"]:
        raise ValueError("Missing or changed original receipt: " + str(path))
    return path


def passing_xml(report, *, required_names=None):
    cases = ElementTree.parse(report).getroot().findall(".//testcase")
    if not cases or any(case.find(tag) is not None for case in cases for tag in ("skipped", "failure", "error")):
        raise ValueError("Actual gate has missing, failed or skipped cases")
    if required_names is not None and [case.attrib["name"] for case in cases] != required_names:
        raise ValueError("Actual required selection differs from the original selector")
    return {"passed": len(cases), "skipped": 0, "cases": [{"classname": case.attrib.get("classname"), "name": case.attrib["name"]} for case in cases]}


def original_run_receipt(directory, *, phase):
    receipt = read_required(directory, "main-command.json")
    if receipt["returncode"] != 0 or not receipt["natural_exit"] or hashlib.sha256((directory / "main.log").read_bytes()).hexdigest() != receipt["log_sha256"]:
        raise ValueError("Required execution did not exit naturally with its original log")
    selector = "tests/fleet/test_bc10_fleet_unified_task_experience.py::" + REQUIRED_CASES[phase]
    if selector not in receipt["argv"]:
        raise ValueError("Required original selector not executed")
    report = directory / ("main.xml" if phase == "main" else "boundary.xml")
    counts = passing_xml(report, required_names=[REQUIRED_CASES[phase]])
    cleanup = read_required(directory, "owned-cleanup.json")
    if cleanup["errors"] or cleanup["remaining"] or cleanup["live_handles"] or cleanup["retained_schema_for_unresolved_owned_execution"]:
        raise ValueError("Required owned cleanup remains unresolved")
    if phase == "main":
        if cleanup["worker_exit_codes"] != [0, 0] or cleanup["browser_exit_code"] != 0 or cleanup["next_exit_code"] != -15:
            raise ValueError("Required original main processes did not settle")
    else:
        observations = read_required(directory, "boundary-observations.json")
        restart = next(action for action in observations["actions"] if action["action"] == "stock-journal-restart")
        local_stop = read_required(directory, "boundary-partition-local-stop.json")
        worker = local_stop["worker_containers"][1]
        if cleanup["worker_exit_codes"] != [0, worker["State"]["ExitCode"], restart["exit_code"]] or worker["State"]["Running"] or restart["exit_code"] is None:
            raise ValueError("Original partition/recovery worker process receipts did not settle")
    schema = read_required(directory, "owned-schema.json")
    if not schema["created"] or not schema["dropped"]:
        raise ValueError("Required owned schema not cleaned")
    return counts


def fixture_import_closure(repository, paths):
    # Include the current test helper closure conservatively. The selected
    # historical tests retain their original byte hashes and execution scope.
    pending = [path for path in paths if path.startswith("backend/tests/")]
    found = set(pending)
    if pending:
        for common in ("backend/tests/fleet/conftest.py", "backend/tests/fleet/__init__.py", "backend/tests/__init__.py"):
            if (repository / common).is_file() and common not in found:
                found.add(common)
                pending.append(common)
    while pending:
        path = pending.pop()
        source = repository / path
        if not source.is_file():
            continue
        module = path.removeprefix("backend/").removesuffix(".py").split("/")
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node, ast.ImportFrom):
                prefix = module[: -node.level] if node.level else []
                parts = prefix + (node.module or "").split(".")
                choices = [parts] if node.module else [prefix + [alias.name] for alias in node.names]
            elif isinstance(node, ast.Import):
                choices = [alias.name.split(".") for alias in node.names]
            else:
                continue
            for parts in choices:
                if parts and parts[0] == "fleet":
                    parts = ["tests", *parts]
                if not parts or parts[0] != "tests":
                    continue
                candidates = ["backend/" + "/".join(parts) + ".py"]
                candidates.extend("backend/" + "/".join(parts[:length]) + "/__init__.py" for length in range(1, len(parts) + 1))
                for imported in candidates:
                    if imported not in found and (repository / imported).is_file():
                        found.add(imported)
                        pending.append(imported)
    return found


def committed_hashes(repository, commit, paths):
    ordered = sorted(paths)
    process = subprocess.run(["git", "cat-file", "--batch"], input="".join(commit + ":" + path + "\n" for path in ordered).encode(), cwd=repository, capture_output=True, check=True)
    payload, offset, result = process.stdout, 0, {}
    for path in ordered:
        end = payload.index(b"\n", offset)
        header = payload[offset:end].split()
        offset = end + 1
        if header[-1] == b"missing":
            result[path] = None
            continue
        size = int(header[-1])
        result[path] = hashlib.sha256(payload[offset : offset + size]).hexdigest()
        offset += size + 1
    return result


def qualify_carried_sources(qualification):
    baseline_path = verified_reference(qualification["baseline"])
    baseline = json.loads(baseline_path.read_text())
    for key in qualification["baseline_keys"]:
        baseline = baseline[key]
    required = set()
    for source_ref in qualification["required_input_maps"]:
        sources = json.loads(verified_reference(source_ref["reference"]).read_text())
        for key in source_ref["keys"]:
            sources = sources[key]
        required.update(sources)
    if qualification.get("selected_xml"):
        report = verified_reference(qualification["selected_xml"])
        required.update("backend/" + case.attrib["classname"].replace(".", "/") + ".py" for case in ElementTree.parse(report).getroot().findall(".//testcase"))
    if qualification["include_fixture_closure"]:
        required.update(fixture_import_closure(Path(qualification["repository"]), required))
    entries = qualification["entries"]
    if {entry["path"] for entry in entries} != required:
        raise ValueError("Carried participating path mapping omits actual selected inputs")
    if qualification.get("baseline_commit"):
        if baseline != committed_hashes(Path(qualification["repository"]), qualification["baseline_commit"], required):
            raise ValueError("Carried source baseline does not match accepted Git commit bytes")
    if not entries or len({entry["path"] for entry in entries}) != len(entries):
        raise ValueError("Carried participating source mapping missing or duplicated")
    statuses = Counter()
    for entry in entries:
        prior = baseline.get(entry["path"])
        current_path = Path(qualification["repository"]) / entry["path"]
        current = hashlib.sha256(current_path.read_bytes()).hexdigest() if current_path.is_file() else None
        status = "new" if prior is None else ("unchanged" if prior == current else "changed")
        if entry["baseline_sha256"] != prior or entry["current_sha256"] != current or entry["status"] != status:
            raise ValueError("Carried source mapping does not match original/current bytes")
        if status != "unchanged":
            if not entry["later_coverage"]:
                raise ValueError("Changed participating input lacks explicit later accepted scope")
            for coverage in entry["later_coverage"]:
                verified_reference(coverage["reference"])
                commit = coverage["accepted_commit_or_current_bc10"]
                if commit != "current BC10":
                    if committed_hashes(Path(qualification["repository"]), commit, [entry["path"]])[entry["path"]] != current:
                        raise ValueError("Later accepted commit does not contain current participating bytes")
                if not coverage["scope"] or not coverage["accepted_commit_or_current_bc10"]:
                    raise ValueError("Later source coverage scope missing")
        statuses[status] += 1
    if qualification["scope"] != "original carried gate scope; later accepted slices and current BC10 provide incremental coverage, not a fresh current full gate":
        raise ValueError("Carried source qualification overstates execution scope")
    return dict(statuses)


def collect_carried(proof):
    if proof["scope"] != "carried; not fresh BC10 execution":
        raise ValueError("Carried proof scope changed")
    if not proof.get("source_qualifications"):
        raise ValueError("Original carried source qualification missing")
    rows = []
    for gate in proof["gates"]:
        receipt = json.loads(verified_reference(gate["receipt"]).read_text())
        log = verified_reference(gate["log"])
        if gate["format"] == "xml-with-gate-receipt":
            matches = [row for row in receipt if row["gate"] == gate["gate_name"]]
            if len(matches) != 1 or matches[0]["exit_code"] != 0 or matches[0]["sha256"] != hashlib.sha256(log.read_bytes()).hexdigest():
                raise ValueError("Original carried gate exit/log receipt invalid")
            xml = verified_reference(gate["xml"])
            if matches[0]["xml_sha256"] != hashlib.sha256(xml.read_bytes()).hexdigest():
                raise ValueError("Original carried XML receipt changed")
            counts = passing_xml(xml)
        elif gate["format"] == "natural-selector-log":
            argv = receipt.get("argv", receipt.get("command"))
            if type(receipt.get("natural_exit")) is not int or receipt["natural_exit"] != 0 or gate["selector"] not in argv:
                raise ValueError("Original carried natural exit or exact selector invalid")
            summaries = re.findall(r"(?m)^(?:=+ )?(\d+) passed(?:, [^\n]*)? in [^\n]+", log.read_text())
            if summaries != ["1"] or re.search(r"\b\d+ (?:failed|error|skipped)\b", log.read_text()):
                raise ValueError("Original carried log does not prove exactly one passing case without skips")
            counts = {"passed": 1, "skipped": 0, "selector": gate["selector"], "xml": "not generated in original execution"}
        else:
            raise ValueError("Unsupported carried original evidence format")
        rows.append({"gate": gate["gate_name"], **counts})
    if not rows:
        raise ValueError("Original carried gate missing")
    return {"scope": proof["scope"], "gates": rows, "source_status_counts": [qualify_carried_sources(qualification) for qualification in proof["source_qualifications"]]}


def collect_release(directory):
    inputs = read_required(directory, "release-inputs.json")
    fixture_qualification = json.loads(verified_reference(inputs["original_fixture_provenance"]).read_text())
    fixture = Path(__file__).resolve().parent / "test_bc10_fleet_unified_task_experience.py"
    if hashlib.sha256(fixture.read_bytes()).hexdigest() != fixture_qualification["current_fixture_sha256"]:
        raise ValueError("Qualified boundary-only fixture bytes changed")
    original_fixture = verified_reference(inputs["executed_main_fixture"])
    if hashlib.sha256(original_fixture.read_bytes()).hexdigest() != fixture_qualification["main13_fixture_sha256"]:
        raise ValueError("Original executed main fixture changed")

    class NormalMain(ast.NodeTransformer):
        def visit_AsyncFunctionDef(self, node):
            return None if node.name == "boundary_lifecycle" else self.generic_visit(node)

        def visit_IfExp(self, node):
            if isinstance(node.test, ast.BoolOp) and isinstance(node.test.op, ast.And) and isinstance(node.test.values[0], ast.Name) and node.test.values[0].id == "boundary":
                return self.visit(node.orelse)
            return self.generic_visit(node)

    if ast.dump(NormalMain().visit(ast.parse(original_fixture.read_text()))) != ast.dump(NormalMain().visit(ast.parse(fixture.read_text()))):
        raise ValueError("Current fixture changed the original main13 normal path")
    main, boundary = Path(inputs["main_directory"]), Path(inputs["boundary_directory"])
    main_selection = original_run_receipt(main, phase="main")
    boundary_selection = original_run_receipt(boundary, phase="boundary")
    main_facts, boundary_facts = collect_main(main), collect_boundary(boundary)
    if not main_facts["c_b_c_across_nodes"]:
        raise ValueError("Original across-node C/B/C not observed")
    for facts in (main_facts, boundary_facts):
        if any(facts[key] for key in ("duplicate_effects", "thread_double_writes", "capacity_leaks")):
            raise ValueError("Original effects/writer/capacity invariants failed")
    browser_log = (main / "browser.log").read_text()
    browser, _ = json.JSONDecoder().raw_decode(browser_log[browser_log.index("{") :])
    if browser["stats"]["expected"] != 1 or any(browser["stats"][key] for key in ("unexpected", "skipped", "flaky")):
        raise ValueError("Actual same-flow browser did not pass its required case")
    carried = read_required(directory, "carried-b-c.json")
    if set(carried) != {"B", "C"}:
        raise ValueError("Both carried B/C scopes required")
    if len(carried["B"]["gates"]) != 1 or carried["B"]["gates"][0]["gate_name"] != "b-gate":
        raise ValueError("Original B gate selection missing")
    c_selectors = {"tests/fleet/test_c12_remote_agent_operations.py::test_c12_production_gateway_runner_main", "tests/fleet/test_c12_remote_agent_operations.py::test_c12_production_runner_revocation_and_delivery_recovery"}
    if len(carried["C"]["gates"]) != 2 or {gate["selector"] for gate in carried["C"]["gates"]} != c_selectors:
        raise ValueError("Original C12 main/fault selections missing")
    return {
        "scope": "fresh BC10 main and ONE boundary with source-qualified original carried B/C scopes",
        "original_main_fixture_qualification": fixture_qualification,
        "main_selection": main_selection,
        "boundary_selection": boundary_selection,
        "main": main_facts,
        "boundary": boundary_facts,
        "browser": browser["stats"],
        "carried": {label: collect_carried(proof) for label, proof in carried.items()},
    }


def test_bc_combined_release_aggregate():
    evidence = Path(os.environ["BC10_EVIDENCE_DIR"])
    collected = collect_release(evidence)
    (evidence / "combined-release-derived.json").write_text(json.dumps(collected, indent=2))
