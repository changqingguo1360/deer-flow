"""BC08 native scheduler C/B/C, with original authenticated admission and STOP.

This composes accepted native fixtures, not the full installed bootstrap. The
child composition adds the production scheduled callback and approved slot
scope that the older BC02 fixture deliberately did not install.
"""

import asyncio
import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI, Request
from sqlalchemy import text


def install_receipt_provider(provider, receipts):
    """Use the real SDK; only synthetic model decisions live at this HTTP peer."""
    import tiktoken

    encoding = tiktoken.get_encoding("cl100k_base")
    state = {"continuation": False}

    @provider.post("/v1/chat/completions")
    async def complete(request: Request):
        data = await request.json()
        assert data["model"] == "bc07-controlled-cl100k" and not data.get("stream")
        previous = [message for message in data["messages"] if message.get("tool_calls")]
        if state["continuation"]:
            message = {
                "role": "assistant",
                "content": "Consumed both accepted scheduled child results; the scheduled goal is complete.",
            }
        elif not previous:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "bc08-submit-" + slot,
                        "type": "function",
                        "function": {
                            "name": "submit_fleet_job",
                            "arguments": json.dumps(
                                {
                                    "task_name": "scheduled-" + slot,
                                    "profile": "batch",
                                    "job_slot": slot,
                                    "argv": [
                                        "/bin/sh",
                                        "-c",
                                        "printf 'accepted scheduled child result' > report.txt",
                                    ],
                                    "link_mode": "awaited",
                                }
                            ),
                        },
                    }
                    for slot in ("first", "second")
                ],
            }
        elif len(previous) == 1:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "bc08-await",
                        "type": "function",
                        "function": {"name": "await_fleet_jobs", "arguments": "{}"},
                    },
                    {
                        "id": "bc08-sibling",
                        "type": "function",
                        "function": {"name": "settled_sibling", "arguments": "{}"},
                    },
                ],
            }
        else:
            raise AssertionError("Original cooperative yield made an unexpected third provider call")
        input_tokens = len(encoding.encode(json.dumps(data, ensure_ascii=False, separators=(",", ":"))))
        output_tokens = len(encoding.encode(json.dumps(message, ensure_ascii=False, separators=(",", ":"))))
        usage = {
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        }
        assert output_tokens <= data.get("max_completion_tokens", data.get("max_tokens", 0))
        receipts.append({"request": data, "response": message, "usage": usage})
        if directory := os.environ.get("BC08_EVIDENCE_DIR"):
            Path(directory, "provider-http-receipts.json").write_text(json.dumps(receipts, indent=2))
        return {
            "id": "bc08-" + str(len(receipts)),
            "object": "chat.completion",
            "created": 1,
            "model": data["model"],
            "choices": [
                {
                    "index": 0,
                    "message": message,
                    "finish_reason": "tool_calls" if message.get("tool_calls") else "stop",
                }
            ],
            "usage": usage,
        }

    return state


async def execute_scheduled_initial(payload):
    """Original native child entry, adding exactly the production scheduler seams.

    Both initial and continuation processes use this entry. A continuation has
    no forged scheduled metadata: its actual original LaunchSpec is preserved.
    """
    from unittest.mock import patch

    import deerflow.mcp.tasks.fleet_runtime as fleet_scope
    import deerflow.runtime.runs.worker as worker
    import fleet.test_bc07_fleet_agent_job_dependencies as budget_fixture
    from app.fleet.scheduled_agent_tasks import FleetScheduledAgentTasks
    from app.scheduler.service import ScheduledTaskService
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.persistence.scheduled_task_runs.sql import ScheduledTaskRunRepository
    from deerflow.persistence.scheduled_tasks.sql import ScheduledTaskRepository

    original_scope = fleet_scope.fleet_job_submitter_scope
    original_context = worker.RunContext
    original_contract = budget_fixture.controlled_contract

    def controlled_contract():
        # Historical accepted image/model references stay untouched. This is
        # the synthetic HTTP peer contract, bound to the real launch version.
        return original_contract() | {"version": payload["spec"]["model_version"]}

    def slots(submitter, *, profile_names=(), scheduled_job_slots=None):
        return original_scope(
            submitter,
            profile_names=profile_names,
            scheduled_job_slots=payload["fleet"]["scheduled_job_slots"],
        )

    def scheduled_context(**kwargs):
        thread_store = kwargs["thread_store"]
        sf, capability = thread_store._sf, thread_store._mutation_capability
        repository = RunRepository(sf, mutation_capability=capability)

        async def no_new_admission(**unused):
            raise RuntimeError("Original private runner cannot admit a new scheduled occurrence")

        from deerflow_ecs_fleet.config import FleetConfig

        aggregate = FleetScheduledAgentTasks(sf, FleetConfig.model_validate(payload["fleet"]))
        scheduled = ScheduledTaskService(
            task_repo=ScheduledTaskRepository(sf, run_repository=repository, mutation_capability=capability),
            task_run_repo=ScheduledTaskRunRepository(sf, run_repository=repository, mutation_capability=capability),
            launch_run=no_new_admission,
            aggregate_completion_pending=aggregate.suppress_parent_completion,
            aggregate_completion_outcome=aggregate.completion_outcome,
            poll_interval_seconds=60,
            lease_seconds=60,
            max_concurrent_runs=1,
            queue_timeout_seconds=3600,
        )
        return original_context(**kwargs, on_run_completed=scheduled.handle_run_completion)

    with (
        patch.object(fleet_scope, "fleet_job_submitter_scope", slots),
        patch.object(worker, "RunContext", scheduled_context),
        patch.object(budget_fixture, "controlled_contract", controlled_contract),
    ):
        await budget_fixture.original_http_child(payload)


async def compose_scheduled_case(directory, patch, *, schedule_type="cron"):
    """C10's real auth/scheduler host plus the original C05 checkpoint owner."""
    from deerflow_ecs_fleet.launch_spec import WorkerCompatibility

    from . import test_b02_fleet_foundation as foundation
    from . import test_c10_remote_agent_admission as c10
    from .c04_integration_fixture import node_server
    from .test_bc03_fleet_agent_job_continuations import fleet_database
    from .test_c02_remote_agent_admission import admission
    from .test_c05_remote_agent_runtime import checkpoint_owner

    @asynccontextmanager
    async def fixture(generator):
        value = await anext(generator)
        try:
            yield value
        finally:
            await generator.aclose()

    directory.mkdir(parents=True)
    stack = AsyncExitStack()
    original_settings = foundation.settings

    def approved_settings(path):
        return original_settings(path).model_copy(
            update={
                "continuations_enabled": True,
                "scheduling_mode": "reserved",
                "reserved_job_profile": "batch",
                "scheduled_job_slots": {"first": "batch", "second": "batch"},
                "task_run_limit": 3,
                "task_job_limit": 2,
                "task_token_limit": 1_000_000,
            }
        )

    patch.setattr(foundation, "settings", approved_settings)
    try:
        database = await stack.enter_async_context(fixture(fleet_database.__wrapped__()))
        admitted = await stack.enter_async_context(fixture(admission.__wrapped__(database, directory)))
        host = await stack.enter_async_context(fixture(c10.c10_environment.__wrapped__(admitted, patch, directory)))
        from app.gateway.routers.fleet_nodes import router as node_router

        host.app.include_router(node_router)
        runtime = host.runtime
        # Operator fixture configuration is fixed before scheduled admission;
        # no admitted goal, launch spec, or durable metadata is changed here.
        from deerflow_ecs_fleet.config import FleetConfig

        approved = runtime.config.model_dump(mode="json")
        approved["agent_bindings"]["remote"]["continuation_budget"] = 2
        runtime.config = FleetConfig.model_validate(approved)
        host.app.state.fleet_ownership.config = runtime.config
        host.app.state.fleet_routing_config = runtime.config
        host.app.state.fleet_scheduler_tickets.config = runtime.config
        runtime.scheduler.config = runtime.config
        worker = WorkerCompatibility.model_validate(host.compatibility)
        await runtime.nodes.register(
            node_id="node-c03",
            name="node-c03",
            cpu_millis=4000,
            memory_mib=8192,
            agent_limit=1,
            profile_allowlist=["remote", "batch"],
        )
        opened = await runtime.nodes.open_session("node-c03", protocol_version=1)
        session_id = opened["node_session_id"]
        await runtime.nodes.heartbeat("node-c03", node_session_id=session_id, protocol_version=1)
        await runtime.nodes.advertise(
            "node-c03",
            node_session_id=session_id,
            kind="mixed",
            compatibility=worker.model_dump(mode="json"),
        )
        credential = await runtime.credentials.issue("node-c03", lifetime_seconds=600)
        url = await stack.enter_async_context(node_server(host.app))
        client = await stack.enter_async_context(httpx.AsyncClient(base_url=url, headers=host.headers, cookies={"csrf_token": "c10-csrf"}))
        created = await client.post(
            "/api/scheduled-tasks",
            json={
                "title": "BC08 recurring aggregate goal" if schedule_type == "cron" else "BC08 once aggregate goal",
                "prompt": "Submit two named child jobs and consume their accepted results before completing the goal.",
                "thread_id": "thread-c10-http",
                "context_mode": "reuse_thread",
                "schedule_type": schedule_type,
                "schedule_spec": {"cron": "* * * * *"} if schedule_type == "cron" else {"run_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat()},
                "timezone": "UTC",
                "execution": {"preference": "remote", "profile": "remote"},
            },
        )
        assert created.status_code == 200, created.text
        schedule_id = created.json()["id"]
        triggered = await client.post("/api/scheduled-tasks/" + schedule_id + "/trigger")
        assert triggered.status_code == 200, triggered.text
        assert triggered.json() == {"id": schedule_id, "triggered": True}
        occurrence = await host.app.state.scheduled_task_run_repo.get_active_run(schedule_id)
        assert occurrence is not None and occurrence["status"] == "running"
        initial = {"task_run_id": occurrence["id"], "run_id": occurrence["run_id"]}
        record = await host.app.state.run_manager.get(occurrence["run_id"], user_id=host.user.id)
        assert record.store_only and record.task is None
        env = (
            database[0],
            database[1],
            host.user,
            runtime,
            host.app,
            record,
            session_id,
            credential,
            worker,
        )
        item = await stack.enter_async_context(fixture(checkpoint_owner.__wrapped__(env)))
        item.host, item.client, item.schedule_id, item.initial = (
            host,
            client,
            schedule_id,
            initial,
        )
        async with item.engine.connect() as connection:
            metadata = await connection.scalar(
                text("SELECT metadata_json FROM runs WHERE run_id=:run"),
                {"run": item.spec.run_id},
            )
        assert metadata["scheduled_task_id"] == schedule_id and metadata["scheduled_task_run_id"] == initial["task_run_id"]
        assert item.spec.normalized_config["context"]["scheduled_task_id"] == schedule_id
        assert item.spec.normalized_config["context"]["scheduled_context_mode"] == "reuse_thread"
        return stack, item
    except BaseException:
        await stack.aclose()
        raise


async def sql_observation(item, schedule_id):
    """Read original records and ledger; never write test-owned expected state."""
    async with item.engine.connect() as connection:
        parent = dict(
            (
                await connection.execute(
                    text("SELECT * FROM scheduled_tasks WHERE id=:id"),
                    {"id": schedule_id},
                )
            )
            .mappings()
            .one()
        )
        occurrences = [
            dict(row)
            for row in (
                await connection.execute(
                    text("SELECT * FROM scheduled_task_runs WHERE task_id=:id ORDER BY created_at,id"),
                    {"id": schedule_id},
                )
            ).mappings()
        ]
        runs = [dict(row) for row in (await connection.execute(text("SELECT run_id,status,metadata_json FROM runs ORDER BY created_at,run_id"))).mappings()]
        tasks = [dict(row) for row in (await connection.execute(text("SELECT id,state,current_run_id,generation FROM fleet_agent_tasks ORDER BY created_at,id"))).mappings()]
        jobs = [dict(row) for row in (await connection.execute(text("SELECT id,dedupe_group,state,accepted_manifest_id,source_run_id FROM fleet_jobs ORDER BY id"))).mappings()]
        budgets = [dict(row) for row in (await connection.execute(text("SELECT * FROM fleet_task_budgets ORDER BY agent_task_id"))).mappings()]
        held = await connection.scalar(text("SELECT count(*) FROM fleet_scheduler_tickets WHERE state='held'"))
        executing = await connection.scalar(text("SELECT count(*) FROM scheduled_task_runs WHERE status IN ('launching','running')"))
        original_age = await connection.scalar(
            text("SELECT extract(epoch from clock_timestamp()-created_at) FROM scheduled_task_runs WHERE id=:id"),
            {"id": occurrences[-1]["id"]},
        )
    observed = {
        "parent": parent,
        "occurrences": occurrences,
        "runs": runs,
        "tasks": tasks,
        "jobs": jobs,
        "budgets": budgets,
        "held_tickets": held,
        "executing_occurrences": executing,
        "last_occurrence_age_seconds": original_age,
    }
    if directory := os.environ.get("BC08_EVIDENCE_DIR"):
        Path(
            directory,
            "scheduler-state-" + str(len(runs)) + "-" + str(len(occurrences)) + ".json",
        ).write_text(json.dumps(observed, default=str, indent=2))
    return observed


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc08_original_scheduled_goal_queues_next_occurrence(tmp_path, monkeypatch):
    from app.fleet.continuations import FleetContinuations

    from .c04_integration_fixture import node_server
    from .test_bc02_fleet_agent_job_dependencies import (
        test_bc02_original_agent_yields_then_waits_for_actual_stop,
    )
    from .test_bc03_fleet_agent_job_continuations import complete_children
    from .test_bc07_fleet_agent_job_dependencies import execute_budget_continuation

    provider, receipts = FastAPI(), []
    provider_state = install_receipt_provider(provider, receipts)
    root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join(
            [
                str(root / "tests"),
                str(root),
                str(root / "packages/ecs-fleet"),
                os.environ.get("PYTHONPATH", ""),
            ]
        ),
    )
    subprocess_exec = asyncio.create_subprocess_exec

    async def launch(*argv, **kwargs):
        # The original helpers still launch/own their native subprocesses. Only
        # their fixture entry changes to install original production callbacks.
        argv = tuple(
            value.replace(
                "from fleet.test_bc02_fleet_agent_job_dependencies import original_child",
                "from fleet.test_bc08_fleet_agent_job_continuations import execute_scheduled_initial as original_child",
            )
            if isinstance(value, str)
            else value
            for value in argv
        )
        return await subprocess_exec(*argv, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    async with node_server(provider) as provider_url:
        monkeypatch.setenv("BC07_PROVIDER_URL", provider_url)
        stack, item = await compose_scheduled_case(tmp_path / "main", monkeypatch)
        try:
            initial = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path / "main", monkeypatch, continuation_case="bc08")
            assert initial["old_run_status"] == "success" and initial["task_state"] == "waiting_jobs"
            assert initial["child_exit"] == 0 and initial["stopped_ack"] and initial["reservation_state"] == "released"
            before = await sql_observation(item, item.schedule_id)
            assert before["occurrences"][0]["status"] == "success"
            assert len(before["jobs"]) == 2 and len({job["id"] for job in before["jobs"]}) == 2
            assert len({job["dedupe_group"] for job in before["jobs"]}) == 2 and all(job["dedupe_group"] for job in before["jobs"])
            assert before["budgets"][0]["admitted_runs"] == 1 and before["budgets"][0]["submitted_jobs"] == 2
            due = before["parent"]["next_run_at"]
            if isinstance(due, str):
                due = datetime.fromisoformat(due)
            if due.tzinfo is None:
                due = due.replace(tzinfo=UTC)
            await item.host.app.state.scheduled_task_service.run_once(now=due)
            waiting = await sql_observation(item, item.schedule_id)
            assert len(waiting["occurrences"]) == 2
            queued = waiting["occurrences"][1]
            assert queued["trigger"] == "scheduled" and queued["scheduled_for"] == due
            assert queued["status"] == "queued", "A successful initial run is not completion of its waiting aggregate goal"
            assert queued["run_id"] is None and queued["attempt_count"] == 0
            assert len(waiting["runs"]) == len(before["runs"]) == 1
            assert waiting["executing_occurrences"] == waiting["held_tickets"] == 0
            assert waiting["budgets"] == before["budgets"]
            responses = await asyncio.gather(*(item.client.post("/api/scheduled-tasks/" + item.schedule_id + "/trigger") for _ in range(3)))
            assert all(response.status_code == 200 for response in responses), [response.text for response in responses]
            assert all(response.json() == {"id": item.schedule_id, "triggered": True} for response in responses)
            repeated = await sql_observation(item, item.schedule_id)
            assert len(repeated["occurrences"]) == 2 and len(repeated["runs"]) == 1
            assert repeated["occurrences"][1]["id"] == queued["id"] and repeated["occurrences"][1]["status"] == "queued"
            assert repeated["occurrences"][1]["created_at"] == queued["created_at"]
            assert repeated["occurrences"][1]["attempt_count"] == queued["attempt_count"]
            assert repeated["last_occurrence_age_seconds"] >= waiting["last_occurrence_age_seconds"] >= 0
            assert repeated["budgets"] == before["budgets"]
            completed = await complete_children(
                item.env[1],
                item.env[3].config.model_dump(mode="json"),
                item.identity.node_id,
                item.identity.node_session_id,
            )
            assert {row["job_id"] for row in completed} == {row["id"] for row in before["jobs"]}
            assert all(row["pid"] > 0 and row["exit"] == 0 and row["manifest_id"] for row in completed)
            provider_state["continuation"] = True
            continuation = await FleetContinuations(item.env[1], item.env[3].config, item.host.app.state.run_manager).dispatch(initial["group_id"])
            assert continuation.run_id != item.spec.run_id
            final = await execute_budget_continuation(item, tmp_path / "continuation", expected_jobs=2, expected_calls=1)
            assert final["spec"]["agent_task_id"] == item.spec.agent_task_id and final["task"]["state"] == "succeeded"
            assert final["spec"]["source_workspace_point_id"] == initial["workspace_point_id"]
            assert final["new_run"]["status"] == "success" and final["new_run"]["stopped_at"] and final["new_run"]["reservation_state"] == "released"
            assert all(child["manifest_id"] in json.dumps(final["spec"]["input"]) for child in completed)
            settled = await sql_observation(item, item.schedule_id)
            assert settled["occurrences"][1]["status"] == "queued"
            assert settled["budgets"][0]["admitted_runs"] == 2 and settled["budgets"][0]["submitted_jobs"] == 2
            await item.host.app.state.scheduled_task_service.run_once(now=due + timedelta(seconds=1))
            resumed = await sql_observation(item, item.schedule_id)
            same = next(row for row in resumed["occurrences"] if row["id"] == queued["id"])
            assert same["status"] == "running" and same["run_id"] is not None
            assert same["created_at"] == queued["created_at"] and same["scheduled_for"] == queued["scheduled_for"]
            assert same["attempt_count"] == queued["attempt_count"] + 1
            assert len(resumed["occurrences"]) == 2 and len(resumed["runs"]) == 3
            assert resumed["occurrences"][0]["status"] == "success" and resumed["runs"][0]["status"] == "success"
            assert resumed["executing_occurrences"] == 1 and len(resumed["jobs"]) == 2
            assert len(receipts) == 3
            if evidence := os.environ.get("BC08_EVIDENCE_DIR"):
                Path(evidence, "original-process-chain.json").write_text(
                    json.dumps(
                        {
                            "initial": initial,
                            "children": completed,
                            "continuation": final,
                            "queued_before": queued,
                            "queued_after": same,
                        },
                        default=str,
                        indent=2,
                    )
                )
        finally:
            await stack.aclose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bc08_restart_preserves_queue_deadline_and_resolution(tmp_path, monkeypatch):
    from app.fleet.continuations import FleetContinuations
    from app.fleet.ownership import install_fleet_ownership
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.scheduled_task_runs import ScheduledTaskRunRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.runtime import RunManager

    from .c04_integration_fixture import node_server
    from .test_bc02_fleet_agent_job_dependencies import test_bc02_original_agent_yields_then_waits_for_actual_stop
    from .test_bc03_fleet_agent_job_continuations import complete_children
    from .test_bc07_fleet_agent_job_dependencies import execute_budget_continuation

    async def restart_scheduler(item):
        """Fresh real manager/repos; original recovery runs before joined stop."""
        app, sf = item.host.app, item.env[1]
        await app.state.scheduled_task_service.stop()
        app.state.run_store = RunRepository(sf)
        app.state.run_manager = RunManager(store=app.state.run_store)
        install_fleet_ownership(app, sf)
        app.state.scheduled_task_repo = ScheduledTaskRepository(sf, run_repository=app.state.run_store)
        app.state.scheduled_task_run_repo = ScheduledTaskRunRepository(sf, run_repository=app.state.run_store)
        app.state.scheduled_task_service = item.host.scheduler()
        await app.state.scheduled_task_service.start()
        await app.state.scheduled_task_service.stop()
        assert app.state.scheduled_task_service._task is None
        return app.state.scheduled_task_service

    def owner_evidence(patch, name):
        if original := os.environ.get("BC08_EVIDENCE_DIR"):
            directory = Path(original) / name
            directory.mkdir()
            for key in ("BC08_EVIDENCE_DIR", "BC07_EVIDENCE_DIR", "BC03_EVIDENCE_DIR", "BC02_EVIDENCE_DIR"):
                patch.setenv(key, str(directory))

    root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join([str(root / "tests"), str(root), str(root / "packages/ecs-fleet"), os.environ.get("PYTHONPATH", "")]))
    subprocess_exec = asyncio.create_subprocess_exec

    async def launch(*argv, **kwargs):
        argv = tuple(
            value.replace("from fleet.test_bc02_fleet_agent_job_dependencies import original_child", "from fleet.test_bc08_fleet_agent_job_continuations import execute_scheduled_initial as original_child")
            if isinstance(value, str)
            else value
            for value in argv
        )
        return await subprocess_exec(*argv, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", launch)
    provider, receipts = FastAPI(), []
    provider_state = install_receipt_provider(provider, receipts)
    async with node_server(provider) as provider_url:
        monkeypatch.setenv("BC07_PROVIDER_URL", provider_url)
        # One once owner first: this is the changed parent-completion path,
        # rather than an assertion against already-correct same-thread queuing.
        with monkeypatch.context() as patch:
            owner_evidence(patch, "once-owner")
            stack, item = await compose_scheduled_case(tmp_path / "once-owner", patch, schedule_type="once")
            try:
                initial = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path / "once-owner", patch, continuation_case="bc08-once")
                waiting = await sql_observation(item, item.schedule_id)
                assert initial["old_run_status"] == "success" and initial["task_state"] == "waiting_jobs"
                assert initial["child_exit"] == 0 and initial["stopped_ack"] and initial["reservation_state"] == "released"
                assert waiting["occurrences"][0]["status"] == "success"
                assert waiting["parent"]["status"] == "running", "The once parent completed from its initial core success while the original aggregate still awaits children"
                restarted = await restart_scheduler(item)
                recovered = await sql_observation(item, item.schedule_id)
                assert recovered["parent"]["status"] == "running", "Original startup recovery cancelled an unresolved scheduled aggregate"
                assert recovered["occurrences"][0]["id"] == waiting["occurrences"][0]["id"]
                assert recovered["occurrences"][0]["status"] == "success" and recovered["tasks"][0]["state"] == "waiting_jobs"
                assert len(recovered["runs"]) == 1 and recovered["budgets"] == waiting["budgets"]
                completed = await complete_children(item.env[1], item.env[3].config.model_dump(mode="json"), item.identity.node_id, item.identity.node_session_id)
                assert len(completed) == 2 and all(row["pid"] > 0 and row["exit"] == 0 and row["manifest_id"] for row in completed)
                provider_state["continuation"] = True
                continuation = await FleetContinuations(item.env[1], item.env[3].config, item.host.app.state.run_manager).dispatch(initial["group_id"])
                assert continuation.run_id != item.spec.run_id
                final = await execute_budget_continuation(item, tmp_path / "once-continuation", expected_jobs=2, expected_calls=1)
                assert final["task"]["state"] == "succeeded"
                assert final["spec"]["source_workspace_point_id"] == initial["workspace_point_id"]
                assert final["new_run"]["status"] == "success" and final["new_run"]["stopped_at"] and final["new_run"]["reservation_state"] == "released"
                assert all(child["manifest_id"] in json.dumps(final["spec"]["input"]) for child in completed)
                await restarted.run_once(now=datetime.now(UTC))
                resolved = await sql_observation(item, item.schedule_id)
                assert resolved["parent"]["status"] == "completed"
                assert resolved["occurrences"][0]["id"] == waiting["occurrences"][0]["id"] and resolved["occurrences"][0]["status"] == "success"
                # Replay only the genuine original completion; the continuation
                # never receives forged original scheduler metadata or fences.
                # C10 host replay uses the real native trusted-host repository;
                # the child callback above retains its private capability.
                # This does not qualify an installed capability bootstrap.
                original = await item.host.app.state.run_manager.get(item.spec.run_id, user_id=item.spec.user_id)
                assert original.status.value == "success"
                await restarted.handle_run_completion(original)
                replayed = await sql_observation(item, item.schedule_id)
                assert replayed["parent"]["status"] == "completed"
                stable_fields = ("id", "task_id", "thread_id", "status", "run_id", "created_at", "attempt_count", "error")
                assert [{field: row[field] for field in stable_fields} for row in replayed["occurrences"]] == [{field: row[field] for field in stable_fields} for row in resolved["occurrences"]]
                assert replayed["tasks"] == resolved["tasks"] and replayed["budgets"] == resolved["budgets"]
                assert len(replayed["runs"]) == 2 and len(replayed["jobs"]) == 2
                # The same once owner now selects Local through the original
                # authenticated schedule API. A real small LangGraph and the
                # original attached Gateway worker finish before the scheduler
                # records launch bookkeeping, as production already permits.
                from dataclasses import replace

                from langchain_core.messages import AIMessage
                from langgraph.graph import END, START, MessagesState, StateGraph

                from deerflow.extensions import get_loaded_extensions
                from deerflow.runtime.checkpointer.async_provider import make_checkpointer
                from deerflow.runtime.stream_bridge import MemoryStreamBridge

                local_selection = await item.client.patch(
                    "/api/scheduled-tasks/" + item.schedule_id,
                    json={
                        "context_mode": "fresh_thread_per_run",
                        "execution": {"preference": "local"},
                        "schedule_spec": {"run_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat()},
                    },
                )
                assert local_selection.status_code == 200, local_selection.text
                assert local_selection.json()["status"] == "enabled" and local_selection.json()["execution"] == {"preference": "local"}
                local_writer = await stack.enter_async_context(make_checkpointer(item.private))
                graph = StateGraph(MessagesState)
                graph.add_node("local_completion", lambda state: {"messages": [AIMessage(content="The Local scheduled run completed.")]})
                graph.add_edge(START, "local_completion")
                graph.add_edge("local_completion", END)
                local_graph = graph.compile(checkpointer=local_writer)
                original_launch = restarted._launch_run
                local_fast = {}

                async def finish_local_before_bookkeeping(**kwargs):
                    launched = await original_launch(**kwargs)
                    record = await item.host.app.state.run_manager.get(launched["run_id"], user_id=item.spec.user_id)
                    assert not record.store_only and record.task is not None
                    worker_error = None
                    try:
                        await record.task
                    except Exception as error:
                        worker_error = {"type": type(error).__name__, "message": str(error)}
                        raise
                    finally:
                        before_bookkeeping = await sql_observation(item, item.schedule_id)
                        local_fast.update(
                            run_id=record.run_id,
                            thread_id=record.thread_id,
                            actual_worker_status=record.status.value,
                            ownership_lost=record.ownership_lost,
                            worker_error=worker_error,
                            before_bookkeeping=before_bookkeeping,
                        )
                    # No association/timestamp/terminal fields are written here.
                    # Return the actual launch result to its original writer.
                    return launched

                with patch.context() as local_patch:
                    local_patch.setattr(item.host.app.state, "checkpointer", local_writer)
                    local_patch.setattr(item.host.app.state, "stream_bridge", MemoryStreamBridge())
                    # C10 originally needs only service lookup; the actual
                    # Local worker consumes the full original extension view.
                    local_patch.setattr(item.host.app.state, "extensions", replace(get_loaded_extensions(), services=item.host.app.state.extensions.services))
                    local_patch.setattr("app.gateway.services.resolve_agent_factory", lambda _: lambda **kwargs: local_graph)
                    local_patch.setattr(restarted, "_launch_run", finish_local_before_bookkeeping)
                    triggered_local = await item.client.post("/api/scheduled-tasks/" + item.schedule_id + "/trigger")
                assert triggered_local.status_code == 200, triggered_local.text
                after_bookkeeping = await sql_observation(item, item.schedule_id)
                local_fast["after_bookkeeping"] = after_bookkeeping
                local_fast["selection_http"] = local_selection.json()
                if evidence := os.environ.get("BC08_EVIDENCE_DIR"):
                    Path(evidence, "local-fast-original-completion.json").write_text(json.dumps(local_fast, default=str, indent=2))
                local_occurrence = next(row for row in after_bookkeeping["occurrences"] if row["run_id"] == local_fast["run_id"])
                local_core = next(row for row in after_bookkeeping["runs"] if row["run_id"] == local_fast["run_id"])
                assert local_fast["before_bookkeeping"]["parent"]["last_run_id"] == item.spec.run_id
                assert local_core["status"] == local_occurrence["status"] == local_fast["actual_worker_status"] == "success"
                assert local_fast["before_bookkeeping"]["parent"]["status"] == "completed", "The actual Local worker completed before launch bookkeeping but its original completion hook failed to finalize the once parent"
                assert not local_fast["ownership_lost"], "The original Local completion hook incorrectly applied Fleet aggregate association fencing"
                assert after_bookkeeping["parent"]["status"] == "completed" and after_bookkeeping["parent"]["last_run_id"] == local_fast["run_id"]
                assert after_bookkeeping["parent"]["last_thread_id"] == local_fast["thread_id"] != item.spec.thread_id
                assert len(after_bookkeeping["runs"]) == 3 and len(after_bookkeeping["tasks"]) == 1 and len(after_bookkeeping["jobs"]) == 2
                assert after_bookkeeping["budgets"] == replayed["budgets"]
                if evidence := os.environ.get("BC08_EVIDENCE_DIR"):
                    Path(evidence, "once-original-process-chain.json").write_text(
                        json.dumps({"initial": initial, "children": completed, "continuation": final, "waiting": waiting, "recovered": recovered, "resolved": resolved, "completion_replayed": replayed}, default=str, indent=2)
                    )
            finally:
                await stack.aclose()
        # One recurring owner adds exactly the schedule-wide Local/fresh-thread
        # blocker and preserved deadline through original scheduler restart.
        provider_state["continuation"] = False
        with monkeypatch.context() as patch:
            owner_evidence(patch, "recurring-owner")
            stack, item = await compose_scheduled_case(tmp_path / "recurring-owner", patch)
            try:
                initial = await test_bc02_original_agent_yields_then_waits_for_actual_stop(item, tmp_path / "recurring-owner", patch, continuation_case="bc08-recurring")
                assert initial["old_run_status"] == "success" and initial["task_state"] == "waiting_jobs" and initial["stopped_ack"]
                before = await sql_observation(item, item.schedule_id)
                selection = await item.client.patch("/api/scheduled-tasks/" + item.schedule_id, json={"context_mode": "fresh_thread_per_run", "execution": {"preference": "local"}})
                assert selection.status_code == 200, selection.text
                selected = selection.json()
                assert selected["context_mode"] == "fresh_thread_per_run" and selected["thread_id"] is None and selected["execution"]["preference"] == "local"
                due = datetime.fromisoformat(selected["next_run_at"])
                if due.tzinfo is None:
                    due = due.replace(tzinfo=UTC)
                await item.host.app.state.scheduled_task_service.run_once(now=due)
                waiting = await sql_observation(item, item.schedule_id)
                assert len(waiting["occurrences"]) == 2
                queued = waiting["occurrences"][1]
                assert queued["trigger"] == "scheduled" and queued["thread_id"] != item.spec.thread_id
                assert queued["status"] == "queued" and queued["run_id"] is None and queued["attempt_count"] == 0, "The unresolved schedule goal must block even a Local occurrence on another thread"
                assert len(waiting["runs"]) == 1 and waiting["held_tickets"] == waiting["executing_occurrences"] == 0
                assert waiting["budgets"] == before["budgets"]
                restarted = await restart_scheduler(item)
                recovered = await sql_observation(item, item.schedule_id)
                same = next(row for row in recovered["occurrences"] if row["id"] == queued["id"])
                assert same["status"] == "queued" and same["created_at"] == queued["created_at"] and same["attempt_count"] == queued["attempt_count"]
                assert len(recovered["runs"]) == 1 and recovered["budgets"] == waiting["budgets"]
                assert recovered["last_occurrence_age_seconds"] >= waiting["last_occurrence_age_seconds"] >= 0
                # Expire the original row through the existing timeout writer;
                # a full tick at this future time could admit another due row.
                created = queued["created_at"]
                if isinstance(created, str):
                    created = datetime.fromisoformat(created)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=UTC)
                await restarted._expire_waiting_runs(now=created + timedelta(seconds=restarted._queue_timeout_seconds + 1))
                expired = await sql_observation(item, item.schedule_id)
                terminal = next(row for row in expired["occurrences"] if row["id"] == queued["id"])
                assert terminal["status"] == "failed" and terminal["error"] == "scheduled task queue wait timeout exceeded"
                assert terminal["created_at"] == queued["created_at"] and terminal["run_id"] is None
                assert len(expired["occurrences"]) == 2 and len(expired["runs"]) == 1
                assert expired["held_tickets"] == expired["executing_occurrences"] == 0 and expired["budgets"] == waiting["budgets"]
                # Resolve this same stopped waiting owner through its actual
                # authenticated cancel transport; no new executor or STOP.
                from app.fleet.scheduled_agent_tasks import FleetScheduledAgentTasks
                from app.gateway.routers.fleet_agent_tasks import router as owned_task_router

                item.host.app.include_router(owned_task_router)
                owned = expired["tasks"][0]
                cancelled_http = await item.client.post(
                    "/api/threads/" + item.spec.thread_id + "/agent-tasks/" + owned["id"] + "/cancel",
                    json={"expected_generation": owned["generation"], "idempotency_key": "bc08-original-waiting-cancel"},
                )
                assert cancelled_http.status_code == 200, cancelled_http.text
                cancellation = cancelled_http.json()
                assert cancellation["state"] == "cancelled" and cancellation["generation"] == owned["generation"] + 1
                await item.host.app.state.fleet_scheduler_tickets.reconcile()
                async with item.env[1]() as session:
                    receipt = (await session.execute(text("SELECT * FROM fleet_scheduled_agent_tasks WHERE scheduled_task_id=:id"), {"id": item.schedule_id})).mappings().one()
                    operation = (await session.execute(text("SELECT * FROM fleet_task_operation_receipts WHERE id=:id"), {"id": cancellation["operation_id"]})).mappings().one()
                    assert receipt["state"] == "resolved" and receipt["resolution_kind"] == "cancelled" and receipt["resolved_status"] == "cancelled"
                    assert receipt["original_run_id"] == item.spec.run_id and receipt["resolved_run_id"] == item.spec.run_id
                    assert receipt["resolved_operation_id"] == operation["id"] and operation["state"] == "completed"
                    assert receipt["resolved_workspace_point_id"] == initial["workspace_point_id"] == operation["source_workspace_point_id"]
                    assert not await FleetScheduledAgentTasks(item.env[1], item.env[3].config).blocks(session, schedule_id=item.schedule_id)
                    cancellation_proof = {"http": cancellation, "receipt": dict(receipt), "operation": dict(operation)}
                if evidence := os.environ.get("BC08_EVIDENCE_DIR"):
                    Path(evidence, "recurring-original-queue-deadline.json").write_text(
                        json.dumps({"initial": initial, "selection_http": selected, "waiting": waiting, "recovered": recovered, "expired": expired, "cancellation": cancellation_proof}, default=str, indent=2)
                    )
            finally:
                await stack.aclose()
