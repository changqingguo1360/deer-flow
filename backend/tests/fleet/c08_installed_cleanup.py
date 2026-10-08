"""Settle every owned fixture cleanup action, retaining primary failure evidence."""

import asyncio
import inspect


class OwnedCleanupFailures(BaseExceptionGroup):
    """Only fixture-owned aggregates are flattened across nested cleanup scopes."""


async def settle_owned_cleanup(actions, *, original_error=None):
    errors = []
    for _, action in actions:
        try:
            outcome = action()
            if inspect.isawaitable(outcome):
                await outcome
        except BaseException as error:
            errors.append(error)
    if errors:
        if original_error is not None:
            errors[0:0] = list(original_error.exceptions) if isinstance(original_error, OwnedCleanupFailures) else [original_error]
        raise OwnedCleanupFailures("Owned installed fixture cleanup failures", errors)


async def cancel_owned_task(task):
    """Bound cancellation settlement to five seconds; never hide a pending owner."""
    if not task.done():
        task.cancel()
        _, pending = await asyncio.wait({task}, timeout=5)
        if pending:
            raise RuntimeError("Owned task remained pending after five-second cancellation settlement")
    if not task.cancelled():
        return task.result()


async def settle_owned_execution(task):
    """Existing 125-second wait plus five-second cancel cap; separate from Runner FINAL120."""
    if task.done():
        return await task
    try:
        return await asyncio.wait_for(asyncio.shield(task), 125)
    except BaseException as error:
        try:
            await cancel_owned_task(task)
        except BaseException as settlement_error:
            if settlement_error is error:
                raise error
            raise BaseExceptionGroup("Owned execution wait/cancellation failures", [error, settlement_error])
        raise


async def settle_owned_containers(*, refs, discover, driver, execution, before=(), after=(), tail=(), original_error=None):
    """Discover before and after bounded execution; settle each owned ref separately."""
    errors = []
    stopped = set()

    async def actions(items):
        try:
            await settle_owned_cleanup(items)
        except OwnedCleanupFailures as error:
            errors.extend(error.exceptions)

    async def discover_refs():
        found = discover()
        if inspect.isawaitable(found):
            found = await found
        for ref in found:
            if ref not in refs:
                refs.append(ref)

    async def stop_refs():
        for ref in list(refs):
            if ref not in stopped:
                stopped.add(ref)
                await actions([("stop-" + ref, lambda ref=ref: driver.stop(ref))])

    await actions(before)
    await actions([("discover-before", discover_refs)])
    await stop_refs()
    if execution is not None:
        await actions([("execution", lambda: settle_owned_execution(execution))])
    await actions([("discover-after", discover_refs)])
    await stop_refs()
    await actions(after)
    for ref in list(refs):
        await actions([("remove-" + ref, lambda ref=ref: driver.command("rm", "--force", ref, timeout=30))])
    await actions(tail)
    if errors:
        primary = list(original_error.exceptions) if isinstance(original_error, OwnedCleanupFailures) else ([] if original_error is None else [original_error])
        raise OwnedCleanupFailures("Owned installed fixture cleanup failures", primary + errors)
