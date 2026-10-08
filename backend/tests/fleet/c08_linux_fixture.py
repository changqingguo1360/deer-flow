"""Installed observation-only fixture around the original Gateway environment.

The original AgentRunner, stock graph, factories and complete C04 provider/tool
sequence execute. A trusted boundary task calls the real private publisher only
at the original presentation callback or terminal prepared boundary. The current
prepared candidate remains unaccepted until the original Node physically stops
the run; a final observation preserves the earlier accepted partial point.
"""

import asyncio
import json
import time
from pathlib import Path


def receipt(event, **fields):
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def original_writer_ancestry(publisher, spec):
    from deerflow.config.paths import get_paths

    handles = tuple(publisher.controller._process_handles)
    supervisors = {handle.supervisor.pid: handle for handle in handles if handle._registered_supervisor}
    outputs = get_paths().sandbox_outputs_dir(spec.thread_id, user_id=spec.user_id)
    identities = [json.loads(path.read_text()) for path in sorted(outputs.rglob("*.identity.json"))]
    assert len(identities) == 6
    ancestry = []
    for identity in identities:
        current = identity["pid"]
        chain = []
        for _ in range(publisher.controller.pids_limit):
            raw = Path("/proc", str(current), "stat").read_text()
            tail = raw[raw.rfind(")") + 2 :].split()
            process = {"pid": current, "ppid": int(tail[1]), "start_ticks": int(tail[19]), "state": tail[0]}
            chain.append(process)
            if len(chain) == 1:
                assert process["start_ticks"] == identity["start_ticks"]
            if current in supervisors:
                owner = supervisors[current].supervisor
                assert process["start_ticks"] == owner.start_ticks
                ancestry.append(
                    {
                        "writer": identity,
                        "birth_ppid": identity["ppid"],
                        "current_ppid": chain[0]["ppid"],
                        "chain": chain,
                        "supervisor_pid": owner.pid,
                        "supervisor_start_ticks": owner.start_ticks,
                        "original_tool_execution_id": owner.tool_execution_id,
                    }
                )
                break
            assert process["ppid"] > 1
            current = process["ppid"]
        else:
            raise AssertionError("Actual writer ancestry exceeded frozen PID bound")
    return ancestry, handles


class PublicationBeginObserver:
    """Observe only after original settlement, before original sf.begin()."""

    def __init__(self, original, publisher, ancestry, handles):
        self.original, self.publisher = original, publisher
        self.ancestry, self.handles = ancestry, handles
        self.observed = False

    def __getattr__(self, name):
        return getattr(self.original, name)

    def begin(self):
        if not self.observed:
            assert not self.publisher.controller._process_handles
            assert all(handle._closed and handle.process.poll() is not None and all(not thread.is_alive() for thread in handle.pipe_drains) for handle in self.handles)
            absent = []
            for entry in self.ancestry:
                identity = entry["writer"]
                try:
                    raw = Path("/proc", str(identity["pid"]), "stat").read_text()
                except FileNotFoundError:
                    absent.append({"pid": identity["pid"], "start_ticks": identity["start_ticks"], "observation": "absent"})
                else:
                    tail = raw[raw.rfind(")") + 2 :].split()
                    assert int(tail[19]) != identity["start_ticks"]
                    absent.append({"pid": identity["pid"], "start_ticks": identity["start_ticks"], "observation": "old_start_identity_absent", "replacement_start_ticks": int(tail[19])})
            receipt(
                "original-writers-joined-before-publication-sf-begin",
                ancestry=self.ancestry,
                physically_absent=absent,
                original_popen_returncodes=[handle.process.returncode for handle in self.handles],
                actual_pipe_threads_joined=True,
                observed_monotonic=time.monotonic(),
                budget_started=self.publisher.teardown.budget._deadline is not None,
            )
            self.observed = True
        return self.original.begin()


async def build_environment(*, bootstrap, spec, grant):

    from app.fleet.runner_context import build_agent_environment
    from deerflow.mcp.session_pool import McpScopeBarrierClosed
    from fleet.c04_worker_fixture import ScriptedModel

    if bootstrap is not None:
        from fleet.c08_installed_bytes import verify_installed

        verify_installed()
    environment = await build_agent_environment(bootstrap=bootstrap, spec=spec, grant=grant)
    publisher = environment.workspace_publications
    original_reply = ScriptedModel.reply
    original_callback = environment.context.checkpointer.after_root_commit
    original_wait = publisher.wait_prepared
    original_sf = publisher.sf
    pool, scope = publisher.pool, publisher.scope_key
    final = "c08-final-boundary" in str(spec.input)
    observed = {"first": False}

    def observed_reply(model, messages):
        result = original_reply(model, messages)
        for call in result.tool_calls:
            if call["name"] == "bash":
                role = model.model
                call["args"]["command"] += " && python -m fleet.c08_linux_probe --output /mnt/user-data/outputs/" + role + "-extra"
        return result

    async def observed_callback(config, metadata):
        if publisher.controller.pending_presentations and not observed["first"]:
            observed["first"] = True
            snapshot = await publisher.accessor.aget(config)
            names = sorted({getattr(message, "name", "") for message in snapshot.values.get("messages", []) if message.type == "tool"})
            receipt("original-model-paused", tools=names, model="parent", observation="first-original-presentation-callback")
            ancestry, handles = original_writer_ancestry(publisher, spec)
            first = pool._entries[("c04", scope)][2]
            receipt(
                "original-pool-ownership",
                pool_id=id(pool),
                current_singleton_same=__import__("deerflow.mcp.session_pool", fromlist=["get_session_pool"]).get_session_pool() is pool,
                target_scope=scope,
                entries=[{"server": key[0], "scope": key[1], "owner_done": entry[2].done()} for key, entry in pool._entries.items()],
                managed_scopes=sorted(pool._managed_scopes),
            )
            await pool.close_scope_and_join(scope, deadline=publisher.controller.execution_deadline)
            assert first.done()
            session = await pool.get_session("c04", scope, {"transport": "stdio", "command": "/usr/local/bin/python", "args": ["-m", "fleet.c04_mcp_fixture"], "env": {"ERP_AUTH": "c04-target-access"}})
            assert (await session.call_tool("echo", {"value": "c08-original-pool-reconnect"})).content[0].text == "c08-original-pool-reconnect"
            receipt("original-mcp-close-reconnect", original_owner_done=first.done(), new_owner_is_original=pool._entries[("c04", scope)][2] is first)
            publisher.sf = PublicationBeginObserver(original_sf, publisher, ancestry, handles)
        await original_callback(config, metadata)

    async def observed_wait(identity, *, barrier_epoch, deadline):
        candidate = await original_wait(identity, barrier_epoch=barrier_epoch, deadline=deadline)
        target = identity.kind == ("final" if final else "partial")
        if not target:
            return candidate
        assert publisher.controller._closed
        try:
            await pool.get_session("c04", scope, {"transport": "stdio", "command": "must-not-launch"})
        except McpScopeBarrierClosed as error:
            receipt("original-prepared-mcp-rejection", exception_type=type(error).__name__, message=str(error))
        else:
            raise AssertionError("Prepared scope reopened without accepted point")
        receipt("original-request-published", request_id=identity.request_id, epoch=barrier_epoch, kind=identity.kind, checkpoint_id=identity.checkpoint_id, budget_started=publisher.teardown.budget._deadline is not None)
        receipt("original-prepared-gate-closed", manifest_id=candidate.id, kind=identity.kind, epoch=barrier_epoch, budget_started=publisher.teardown.budget._deadline is not None)
        # The real callback/terminal preparation remains suspended at its real
        # prepared candidate until the original Node physically stops this run.
        while True:
            await asyncio.sleep(0.05)

    ScriptedModel.reply = observed_reply
    environment.context.checkpointer.after_root_commit = observed_callback
    publisher.wait_prepared = observed_wait
    original_close = environment.close

    async def close():
        ScriptedModel.reply = original_reply
        await original_close()

    environment.close = close
    return environment


def compatibility():
    from app.fleet.runner_context import installed_compatibility

    return installed_compatibility()


build_environment.worker_compatibility = compatibility
