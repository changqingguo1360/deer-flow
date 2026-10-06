"""Original Fleet authority behind the harness's opaque model-call capability."""

import asyncio
import json

from deerflow_ecs_fleet.task_budgets import TaskBudgetExceeded, record_denial, reserve, settle, unknown

from app.fleet.mutation import FleetMutationCapability, _cancellation_authority, _SessionCursor
from deerflow.models.call_budget import mark_private_call_denial
from deerflow.runtime.execution.mutation_context import OwnershipRejected, current_remote_mutation_context


class FleetModelBudgetCapability:
    def __init__(self, session_factory, identity, spec, *, contracts):
        self.sf, self.identity, self.spec = session_factory, identity, spec
        self._contracts = json.loads(json.dumps(contracts))
        if not isinstance(self._contracts.get(spec.model_name), dict) or self._contracts[spec.model_name].get("version") != spec.model_version:
            raise ValueError("Original launch model budget version conflicts")
        self._mutation = FleetMutationCapability(identity, spec)
        self._owner_loop = asyncio.get_running_loop()
        self._receipt_identity = {
            "agent_task_id": spec.agent_task_id,
            "generation": spec.generation,
            "run_id": spec.run_id,
            "attempt_id": identity.attempt_id,
            "node_id": identity.node_id,
            "node_session_id": identity.node_session_id,
            "token_stamp": identity.token_stamp,
        }
        self._denial_identity = {"agent_task_id": spec.agent_task_id, "user_id": spec.user_id, "thread_id": spec.thread_id, "generation": spec.generation, "source_run_id": spec.run_id}

    def contract(self, model_name, provider_use):
        contract = self._contracts.get(model_name)
        if not isinstance(contract, dict) or contract.get("provider_use") != provider_use:
            raise ValueError("Model has no approved task budget contract")
        return json.loads(json.dumps(contract))

    async def _validate(self, session, *, new_call=False):
        if current_remote_mutation_context() != self._mutation.context:
            raise OwnershipRejected("Original private model-call context required")
        # Reuse the original task/run/placement/node/reservation/attempt fence.
        # Legitimate finishing work remains owned until original physical STOP.
        operation = "control.observe"
        if new_call:
            operation = "mcp.create"
            authority = _cancellation_authority.get()
            if authority is not None and authority.context == self._mutation.context and authority.operation == "checkpoint.cancel_title":
                operation = "thread.checkpoint_title"
        await self._mutation._guard.validate(_SessionCursor(session), thread_id=self.spec.thread_id, operation=operation, allow_terminal=not new_call)

    async def reserve_async(self, request_bound):
        async with self.sf.begin() as session:
            await self._validate(session, new_call=True)
            ticket, reason = await reserve(session, identity=self._receipt_identity, bound=request_bound)
        # A denied decision is committed before exposing the exception.
        if reason is not None:
            await record_denial(self.sf, self._denial_identity, reason, request_digest=request_bound["request_digest"])
            raise TaskBudgetExceeded(reason)
        return ticket

    async def settle_async(self, ticket, measured_usage):
        try:
            async with self.sf.begin() as session:
                await self._validate(session)
                if any(ticket.get(key) != value for key, value in self._receipt_identity.items()):
                    raise OwnershipRejected("Original model settlement identity required")
                await settle(session, ticket=ticket, usage=measured_usage)
        except BaseException:
            await self.mark_unknown(ticket, "usage_or_original_authority_lost")
            raise

    async def mark_unknown(self, ticket, reason):
        if any(ticket.get(key) != value for key, value in self._receipt_identity.items()):
            raise OwnershipRejected("Original uncertain model ticket identity required")
        # Losing authority may retain uncertainty, but never authorizes refund.
        async with self.sf.begin() as session:
            await unknown(session, ticket=ticket, reason=reason)

    async def deny_async(self, reason):
        try:
            async with self.sf.begin() as session:
                await self._validate(session)
            await record_denial(self.sf, self._denial_identity, reason)
            raise TaskBudgetExceeded(reason)
        except BaseException as error:
            # Unsupported cached/factory models have no SDK transport hook.
            # Their original private denial must also bypass error fallbacks.
            mark_private_call_denial(error)
            raise

    def _sync(self, operation, *args):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is self._owner_loop:
            # Do not construct or block on a coroutine from the running owner
            # loop. The async SDK path is mandatory in this context.
            raise OwnershipRejected("Synchronous private model calls require an external thread")
        if self._owner_loop.is_closed() or not self._owner_loop.is_running():
            raise OwnershipRejected("Original model budget owner loop unavailable")
        future = asyncio.run_coroutine_threadsafe(operation(*args), self._owner_loop)
        return future.result()

    def reserve_sync(self, request_bound):
        return self._sync(self.reserve_async, request_bound)

    def settle_sync(self, ticket, measured_usage):
        return self._sync(self.settle_async, ticket, measured_usage)

    def mark_unknown_sync(self, ticket, reason):
        return self._sync(self.mark_unknown, ticket, reason)

    def deny_sync(self, reason):
        try:
            return self._sync(self.deny_async, reason)
        except BaseException as error:
            # The owner-loop/thread preflight itself has no SDK send hook.
            mark_private_call_denial(error)
            raise
