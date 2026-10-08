"""Authenticate public intent against frozen operator routing grants."""

import asyncio

from fastapi import HTTPException

from deerflow.runtime.execution.preference import ExecutionPreference


def approved_binding(config, selection, owner):
    selection = ExecutionPreference.model_validate(selection or {})
    if selection.preference == "local":
        if selection.profile is not None:
            raise HTTPException(422, "Local execution cannot select a remote profile")
        return None
    if selection.profile is None:
        if selection.preference == "remote":
            raise HTTPException(422, "Remote execution requires an approved profile")
        return None
    if config is None or not config.enabled or not config.agents_enabled:
        raise HTTPException(503, "Remote Agent admission unavailable")
    binding = config.agent_bindings.get(selection.profile)
    if binding is None or owner not in binding.allowed_user_ids:
        raise HTTPException(403, "Remote profile is not granted to this user")
    return binding


def validate_execution_preference(app, selection, owner):
    return approved_binding(getattr(app.state, "fleet_routing_config", None), selection, owner)


async def resolve_execution_backend(app, selection, parameters, *, bound=None, ticket=None, lease_owner=None):
    selection = ExecutionPreference.model_validate(selection)
    if bound is not None:
        if selection.preference == "local" or selection.profile is not None and selection.profile != bound.profile_name:
            raise HTTPException(409, "Execution selection conflicts with original bound backend")
        return bound
    config = getattr(app.state, "fleet_routing_config", None)
    binding = approved_binding(config, selection, parameters.user_id)
    if binding is None:
        return None
    from .execution import FleetExecutionBackend
    from .initial_workspace import capture
    from .scheduler_tickets import TicketBackend

    reference = await app.state.fleet_scheduler_tickets.original_workspace(ticket, parameters) if ticket is not None else None
    if reference is None:
        reference = await asyncio.to_thread(capture, config, user_id=parameters.user_id, thread_id=parameters.thread_id)
    backend = FleetExecutionBackend(
        config=config,
        profile_name=selection.profile,
        model_name=binding.model_name,
        model_version=binding.model_version,
        skill_snapshot=binding.compatibility["skill_snapshot"],
        plugin_snapshot=binding.compatibility["plugin_snapshot"],
        workspace_manifest_ref=reference,
        secret_refs=binding.secret_refs,
        continuation_budget=binding.continuation_budget,
    )
    if ticket is not None:
        return TicketBackend(backend, app.state.fleet_scheduler_tickets, ticket, lease_owner)
    return backend
