"""Fail-open notification helpers for extension runtime hooks."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from concurrent.futures import Future, InvalidStateError
from typing import Any

from deerflow_extension_api import (
    EXTENSION_TASK_STORE_KEY,
    CompactionEvent,
    ExtensionData,
    SystemModelRequest,
    SystemModelResult,
    SystemOperationKind,
    TaskInfo,
    TaskOutcome,
)

from deerflow.extensions.registry import LoadedExtensions
from deerflow.runtime.execution.mutation_context import OwnershipRejected, current_remote_mutation_context

logger = logging.getLogger(__name__)


def _validate_extension_scope(extensions):
    bound = getattr(extensions, "mutation_context", None)
    if bound is not None and current_remote_mutation_context() != bound:
        raise OwnershipRejected("Extension notification lost original bound context")
    return bound


def lead_task_id(run_id: str) -> str:
    """Return the stable task id for a lead run, including continuations."""
    return run_id


def lead_task_outcome(*, aborted: bool, succeeded: bool) -> TaskOutcome:
    """Classify a lead run conservatively from its terminal state."""
    if aborted:
        return TaskOutcome.ABORTED
    if succeeded:
        return TaskOutcome.COMPLETED
    return TaskOutcome.FAILED


def subagent_task_outcome(*, cancelled: bool, succeeded: bool) -> TaskOutcome:
    """Classify a subagent execution conservatively from its terminal state."""
    if cancelled:
        return TaskOutcome.ABORTED
    if succeeded:
        return TaskOutcome.COMPLETED
    return TaskOutcome.FAILED


def _host_is_cancelling() -> bool:
    """Whether the host task itself is being cancelled.

    Fail-open has to be decided by the *origin* of a failure, not by its base
    class. ``CancelledError`` reaches a contributor's ``except`` for two very
    different reasons: the host task was cancelled (must propagate), or the
    contributor raised it on its own — an extension implementing an internal
    timeout with cancellation, for instance (must stay contained). Only the
    first increments the task's cancellation counter, so it is what tells the
    two apart.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        # Synchronous hook sites (agent assembly) can run with no loop at all,
        # and "no loop" means there is no host task being cancelled.
        return False
    return task is not None and task.cancelling() > 0


def notify_agent_assembled(descriptor: object, extensions: object | None = None) -> None:
    """Fan a completed assembly out to observers, in registration order.

    Synchronous: agent construction is synchronous and there is no loop to
    dispatch onto. Failures are contained per observer — a broken observer must
    not prevent an agent from being built.
    """
    resolved = extensions
    if resolved is None:
        from deerflow.extensions import get_agent_build_extensions

        resolved = get_agent_build_extensions()
    _validate_extension_scope(resolved)
    observers = getattr(resolved, "agent_assembly_observers", ())
    if not observers:
        return
    app_store = getattr(resolved, "app_store", None)
    for source, observer in observers:
        try:
            observer.on_agent_assembled(app_store, descriptor)
        except asyncio.CancelledError:
            if _host_is_cancelling():
                raise
            # Same rule as the awaited hooks: an observer raising it on its own
            # must not skip its successors, and must not turn graph
            # construction into a deferred interrupt.
            logger.exception(
                "Extension %s: on_agent_assembled raised CancelledError for %s",
                source,
                type(descriptor).__name__,
            )
        except OwnershipRejected:
            raise
        except Exception:
            logger.exception(
                "Extension %s: on_agent_assembled failed for %s",
                source,
                type(descriptor).__name__,
            )


async def _notify_each(
    contributors: tuple[tuple[str, Any], ...],
    hook: str,
    invoke: Callable[[Any], Any],
    task_id: str,
    timeout: float | None,
) -> None:
    """Invoke contributors in order, fail-open, within one shared budget."""
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    for source, contributor in contributors:
        try:
            call = invoke(contributor)
            if deadline is None:
                await call
                continue

            remaining = deadline - loop.time()
            if remaining <= 0:
                close = getattr(call, "close", None)
                if callable(close):
                    close()
                if current_remote_mutation_context() is not None:
                    raise OwnershipRejected("Remote extension notification budget was spent")
                logger.warning(
                    "Extension %s: %s skipped for task %s; the %.1fs notification budget was spent",
                    source,
                    hook,
                    task_id,
                    timeout,
                )
                continue
            await asyncio.wait_for(call, remaining)
        except TimeoutError:
            if deadline is not None and loop.time() >= deadline:
                if current_remote_mutation_context() is not None:
                    raise OwnershipRejected("Remote extension notification exceeded completion budget")
                # Budget exhaustion mid-hook is the same expected operational
                # condition as the skip above, so it stays a warning rather
                # than a hook failure with an asyncio-internal traceback.
                logger.warning(
                    "Extension %s: %s timed out for task %s; the %.1fs notification budget was spent",
                    source,
                    hook,
                    task_id,
                    timeout,
                )
            else:
                # A TimeoutError the contributor raised on its own is a hook
                # failure like any other.
                logger.exception(
                    "Extension %s: %s failed for task %s",
                    source,
                    hook,
                    task_id,
                )
        except asyncio.CancelledError:
            if _host_is_cancelling():
                raise
            # The contributor raised it, so containing it keeps one broken
            # extension from skipping its successors — and, at the task-stop
            # site, from turning a run's cleanup into a deferred interrupt.
            logger.exception(
                "Extension %s: %s raised CancelledError for task %s",
                source,
                hook,
                task_id,
            )
        except OwnershipRejected:
            raise
        except Exception:
            logger.exception(
                "Extension %s: %s failed for task %s",
                source,
                hook,
                task_id,
            )


# Gateway registers its serving loop here. Subagents can run on isolated event
# loops, but extension resources must always be touched on the loop where they
# were started.
_notify_loop: asyncio.AbstractEventLoop | None = None
_pending_dispatches: set[asyncio.Future[Any]] = set()
_dispatch_owners: dict[Any, Any] = {}
_dispatch_cleanup: dict[Any, Future] = {}
_dispatch_lock = threading.RLock()
_dispatch_failures = []
_dispatch_revisions = {}
_warned_no_loop = False
_system_observations_enabled = True


def set_extension_notify_loop(loop: asyncio.AbstractEventLoop | None) -> None:
    """Bind extension notifications to the loop that owns extension resources."""
    global _notify_loop, _system_observations_enabled, _warned_no_loop
    _notify_loop = loop
    _system_observations_enabled = True
    _warned_no_loop = False


def reset_extension_notify_loop() -> None:
    """Remove the process-wide loop binding during host shutdown or tests."""
    global _notify_loop, _system_observations_enabled, _warned_no_loop
    with _dispatch_lock:
        if any(owner is not None for owner in _dispatch_owners.values()):
            raise OwnershipRejected("Remote extension dispatches must drain before reset")
        _notify_loop = None
        _system_observations_enabled = True
        _warned_no_loop = False
        _pending_dispatches.clear()
        _dispatch_owners.clear()
        _dispatch_cleanup.clear()
        _dispatch_failures[:] = [(owner, error) for owner, error in _dispatch_failures if owner is not None]


def suspend_extension_system_observations() -> None:
    """Drop new fire-and-forget observations while awaited hooks still drain."""
    global _system_observations_enabled
    if _notify_loop is not None:
        _system_observations_enabled = False


async def _notify_each_on_extension_loop(
    contributors: tuple[tuple[str, Any], ...],
    hook: str,
    invoke: Callable[[Any], Any],
    task_id: str,
    timeout: float | None,
) -> None:
    loop = _notify_loop
    current_loop = asyncio.get_running_loop()
    if loop is None or loop is current_loop:
        await _notify_each(contributors, hook, invoke, task_id, timeout)
        return
    if not loop.is_running():
        if current_remote_mutation_context() is not None:
            raise OwnershipRejected("Remote extension notification loop unavailable")
        logger.warning(
            "No running loop registered for awaited extension hook; %s for %s was dropped",
            hook,
            task_id,
        )
        return

    owner = current_remote_mutation_context()
    notification = _notify_each(contributors, hook, invoke, task_id, None if owner is not None else timeout)
    try:
        future = _submit_owned_dispatch(notification, loop, owner) if owner is not None else asyncio.run_coroutine_threadsafe(notification, loop)
        with _dispatch_lock:
            cleanup = _dispatch_cleanup.get(future)
    except OwnershipRejected:
        raise
    except Exception as exc:
        notification.close()
        if owner is not None:
            raise OwnershipRejected("Remote awaited notification dispatch unavailable") from exc
        logger.exception(
            "Could not dispatch extension %s for task %s to the registered loop",
            hook,
            task_id,
        )
        return

    try:
        wrapped = asyncio.wrap_future(future)
        if timeout is None:
            await wrapped
        else:
            await asyncio.wait_for(wrapped, timeout)
    except TimeoutError as exc:
        future.cancel()
        if cleanup is not None:
            async with asyncio.timeout(30):
                await asyncio.shield(asyncio.wrap_future(cleanup))
            error = OwnershipRejected("Remote awaited extension dispatch exceeded completion budget")
            with _dispatch_lock:
                _dispatch_failures.append((owner, error))
            raise error from exc
        logger.warning(
            "Extension %s dispatch timed out for task %s after %.1fs",
            hook,
            task_id,
            timeout,
        )
    except asyncio.CancelledError:
        future.cancel()
        if cleanup is not None:
            async with asyncio.timeout(30):
                await asyncio.shield(asyncio.wrap_future(cleanup))
        raise
    except OwnershipRejected:
        raise
    except Exception:
        logger.exception(
            "Extension %s dispatch failed for task %s",
            hook,
            task_id,
        )


async def notify_task_start(
    extensions: LoadedExtensions,
    task_store: ExtensionData,
    info: TaskInfo,
    *,
    timeout: float | None = None,
) -> None:
    _validate_extension_scope(extensions)
    await _notify_each_on_extension_loop(
        extensions.task_lifecycle,
        "on_task_start",
        lambda contributor: contributor.on_task_start(
            extensions.app_store,
            task_store,
            info,
        ),
        info.task_id,
        timeout,
    )


async def notify_task_stop(
    extensions: LoadedExtensions,
    task_store: ExtensionData,
    info: TaskInfo,
    outcome: TaskOutcome,
    *,
    timeout: float | None = None,
) -> None:
    _validate_extension_scope(extensions)
    await _notify_each_on_extension_loop(
        extensions.task_lifecycle,
        "on_task_stop",
        lambda contributor: contributor.on_task_stop(
            extensions.app_store,
            task_store,
            info,
            outcome,
        ),
        info.task_id,
        timeout,
    )


async def notify_system_model_call(
    extensions: LoadedExtensions,
    task_store: ExtensionData | None,
    kind: SystemOperationKind,
    request: SystemModelRequest,
    result: SystemModelResult,
    *,
    timeout: float | None = None,
) -> None:
    """Notify the observers from one immutable extension snapshot."""
    if not extensions.system_model_observers:
        return
    store = task_store if task_store is not None else ExtensionData("detached")
    _validate_extension_scope(extensions)
    await _notify_each_on_extension_loop(
        extensions.system_model_observers,
        "on_system_model_call",
        lambda observer: observer.on_system_model_call(
            extensions.app_store,
            store,
            kind,
            request,
            result,
        ),
        f"{store.scope_id} ({kind.value})",
        timeout,
    )


def task_store_for_system_call(invoke_config: object) -> ExtensionData | None:
    """Recover the live task store from a legacy top-level runtime context."""
    if not isinstance(invoke_config, Mapping):
        return None
    context = invoke_config.get("context")
    if not isinstance(context, Mapping):
        return None
    store = context.get(EXTENSION_TASK_STORE_KEY)
    return store if isinstance(store, ExtensionData) else None


async def observe_system_model_call(
    extensions: LoadedExtensions,
    kind: SystemOperationKind,
    *,
    messages: Any,
    model_name: str | None,
    invoke_config: Any,
    invoke: Callable[[], Awaitable[Any]],
    task_store: ExtensionData | None = None,
    timeout: float | None = None,
) -> Any:
    """Invoke a system-owned model call and report either terminal path."""
    if not extensions.has_system_model_observers:
        return await invoke()

    store = task_store if task_store is not None else task_store_for_system_call(invoke_config)
    request = SystemModelRequest(
        messages=messages,
        model_name=model_name,
        invoke_config=(invoke_config if isinstance(invoke_config, Mapping) else None),
    )
    started = time.monotonic()
    try:
        response = await invoke()
    except asyncio.CancelledError as exc:
        # Cancellation is a terminal path as well: interrupt/rollback admission
        # and shutdown both cancel the run task, so a user sending a follow-up
        # mid-run routinely ends a goal or summarization call here, with the
        # provider tokens already spent. Awaiting observers would be unreliable
        # — a repeated cancel interrupts that await before any of them runs — so
        # this reports through the same non-blocking submission the synchronous
        # memory bridge uses, then propagates the cancellation untouched. A
        # deployment with no registered notify loop drops it, exactly as that
        # bridge does.
        dispatch_system_model_observation(
            notify_system_model_call(
                extensions,
                store,
                kind,
                request,
                SystemModelResult(
                    error=exc,
                    duration_ms=(time.monotonic() - started) * 1000,
                ),
            ),
            kind.value,
            **({"mutation_context": extensions.mutation_context} if getattr(extensions, "mutation_context", None) is not None else {}),
        )
        raise
    except Exception as exc:
        await notify_system_model_call(
            extensions,
            store,
            kind,
            request,
            SystemModelResult(
                error=exc,
                duration_ms=(time.monotonic() - started) * 1000,
            ),
            timeout=timeout,
        )
        raise
    await notify_system_model_call(
        extensions,
        store,
        kind,
        request,
        SystemModelResult(
            response=response,
            duration_ms=(time.monotonic() - started) * 1000,
        ),
        timeout=timeout,
    )
    return response


def dispatch_system_model_observation(
    coro: Coroutine[Any, Any, None],
    what: str,
    *,
    mutation_context=None,
) -> bool:
    """Submit a synchronous call site's observation to the registered loop."""
    global _warned_no_loop

    loop = _notify_loop
    submitted = False
    try:
        if mutation_context is not None and current_remote_mutation_context() != mutation_context:
            raise OwnershipRejected("Detached extension dispatch lost original bound context")
        if not _system_observations_enabled:
            return False
        if loop is None or not loop.is_running():
            if mutation_context is not None:
                raise OwnershipRejected("Remote detached notification loop unavailable")
            if not _warned_no_loop:
                _warned_no_loop = True
                logger.warning(
                    "No running loop registered for extension observations; %s and later ones are dropped",
                    what,
                )
            return False
        try:
            _submit_owned_dispatch(coro, loop, mutation_context or current_remote_mutation_context())
        except OwnershipRejected:
            raise
        except Exception as exc:
            if mutation_context is not None:
                raise OwnershipRejected("Remote detached notification dispatch unavailable") from exc
            logger.debug(
                "Could not dispatch %s to the extension notify loop",
                what,
                exc_info=True,
            )
            return False
        submitted = True
        return True
    finally:
        if not submitted:
            coro.close()


def notify_context_compacted(event: CompactionEvent, extensions: LoadedExtensions | None = None) -> None:
    """Fan a completed compaction out to observers, fire-and-forget.

    The compaction seam sits in the summarization middleware's ``before_model`` /
    ``abefore_model`` hooks; the sync half has no loop to await onto, and the async
    half must not block the model-call turn on observer latency. Both therefore call
    this synchronous entry point, which dispatches to the registered extension-notify
    loop the same non-blocking way a synchronous system-model-call cancellation does,
    reusing the same fail-open cancellation containment inside ``_notify_each``.

    There is no live task for this hook to attach observers to (unlike lifecycle or
    system-model-call notification, which run from an awaited call site holding the
    real task store), so observers receive a detached store — the same fallback
    ``notify_system_model_call`` uses when its caller has none.
    """
    resolved = extensions
    if resolved is None:
        from deerflow.extensions import get_agent_build_extensions

        resolved = get_agent_build_extensions()
    _validate_extension_scope(resolved)
    observers = resolved.context_compaction_observers
    if not observers:
        return
    app_store = resolved.app_store
    task_store = ExtensionData("detached")
    what = f"compaction ({event.transform_kind})"
    dispatch_system_model_observation(
        _notify_each(
            observers,
            "on_context_compacted",
            lambda observer: observer.on_context_compacted(app_store, task_store, event),
            what,
            None,
        ),
        what,
        **({"mutation_context": resolved.mutation_context} if getattr(resolved, "mutation_context", None) is not None else {}),
    )


def _submit_owned_dispatch(coro, loop, owner):
    """Track the actual owner-loop Task through its transaction cleanup.

    Cancelling a concurrent Future only requests Task cancellation. Its early
    done signal must never release SQL resources while Task finally/rollback
    is still running. Only the actual Task done callback releases ownership.
    """
    future, cleanup = Future(), Future()
    with _dispatch_lock:
        _pending_dispatches.add(future)
        _dispatch_owners[future] = owner
        _dispatch_cleanup[future] = cleanup
        if owner is not None:
            _dispatch_revisions[owner] = _dispatch_revisions.get(owner, 0) + 1

    def schedule():
        try:
            task = loop.create_task(coro)
        except BaseException as exc:
            coro.close()
            _finish_dispatch(future, cleanup, error=exc)
            return

        def completed(task):
            error = None if task.cancelled() else task.exception()
            _finish_dispatch(future, cleanup, error=error, cancelled=task.cancelled())

        def cancel_requested(future):
            if future.cancelled() and not task.done():
                loop.call_soon_threadsafe(task.cancel)

        task.add_done_callback(completed)
        future.add_done_callback(cancel_requested)

    try:
        # call_soon_threadsafe captures the dispatching context. create_task
        # inherits that original context even across raw threads / other loops.
        loop.call_soon_threadsafe(schedule)
    except BaseException:
        with _dispatch_lock:
            _pending_dispatches.discard(future)
            _dispatch_owners.pop(future, None)
            _dispatch_cleanup.pop(future, None)
        raise
    return future


def _finish_dispatch(future, cleanup, *, error=None, cancelled=False):
    with _dispatch_lock:
        tracked = future in _dispatch_owners
        owner = _dispatch_owners.pop(future, None)
        _pending_dispatches.discard(future)
        _dispatch_cleanup.pop(future, None)
        if tracked and owner is not None:
            _dispatch_revisions[owner] = _dispatch_revisions.get(owner, 0) + 1
        if error is not None and tracked:
            _dispatch_failures.append((owner, error))
    try:
        if not future.done():
            if error is not None:
                future.set_exception(error)
                future.exception()  # diagnostic retained above
            elif cancelled:
                future.cancel()
            else:
                future.set_result(None)
    except InvalidStateError:
        # Another thread may cancel between done() and result publication.
        pass
    finally:
        cleanup.set_result(None)


def extension_dispatch_failures():
    """Inspectable original failures of detached extension callbacks."""
    current = current_remote_mutation_context()
    with _dispatch_lock:
        return tuple(error for owner, error in _dispatch_failures if owner == current)


async def drain_extension_dispatches(*, timeout=None, deadline=None):
    """Drain owned nested callbacks within the remote completion budget."""
    current = current_remote_mutation_context()
    budget = 30 if current is not None and timeout is None else timeout

    def remaining(limit):
        if deadline is None:
            return limit
        import time

        left = max(0.0, deadline - time.monotonic())
        return left if limit is None else min(limit, left)

    async def wait_for_cleanup(pending):
        with _dispatch_lock:
            completions = tuple(_dispatch_cleanup[future] for future in pending if future in _dispatch_cleanup)
        if completions:
            await asyncio.gather(*(asyncio.shield(asyncio.wrap_future(done)) for done in completions))

    async def cancel_and_settle():
        with _dispatch_lock:
            pending = tuple(future for future in _pending_dispatches if _dispatch_owners.get(future) == current)
        for future in pending:
            future.cancel()
        # This is a cleanup budget, separate from the active work budget. Keep
        # ownership tracked if an operator callback does not finish cancelling.
        async with asyncio.timeout(remaining(30)):
            await wait_for_cleanup(pending)

    try:
        async with asyncio.timeout(remaining(budget)):
            while True:
                with _dispatch_lock:
                    pending = tuple(future for future in _pending_dispatches if _dispatch_owners.get(future) == current)
                if not pending:
                    break
                await wait_for_cleanup(pending)
    except TimeoutError as exc:
        try:
            await cancel_and_settle()
        except TimeoutError:
            pass  # ownership remains tracked; release/reset fail closed
        if current is None:
            raise
        error = OwnershipRejected("Owned extension dispatch drain exceeded completion budget")
        with _dispatch_lock:
            _dispatch_failures.append((current, error))
        raise error from exc
    except asyncio.CancelledError:
        await cancel_and_settle()
        raise
    for error in extension_dispatch_failures():
        if isinstance(error, OwnershipRejected):
            raise error


async def wait_extension_dispatch_cleanup():
    """Wait for actual owned Task rollback markers; host clips total budget.

    No repeat cancellation, separate loop, or successful completion inferred
    from a cancelled concurrent Future. The host keeps this settlement Task
    and all resources retained if its fixed total deadline expires.
    """
    current = current_remote_mutation_context()
    while True:
        with _dispatch_lock:
            completions = tuple(_dispatch_cleanup[future] for future, owner in _dispatch_owners.items() if owner == current and future in _dispatch_cleanup)
        if not completions:
            return
        await asyncio.gather(*(asyncio.shield(asyncio.wrap_future(done)) for done in completions))


def extension_dispatch_revision():
    """Original-scope enqueue/actual-Task completion revision for quiescence."""
    with _dispatch_lock:
        return _dispatch_revisions.get(current_remote_mutation_context(), 0)


def extension_dispatches_pending():
    """Inspect actual owned Task settlement under the original private scope."""
    current = current_remote_mutation_context()
    with _dispatch_lock:
        return any(owner == current for owner in _dispatch_owners.values())


def release_extension_dispatch_failures():
    """Release diagnostics only after this original attempt's resources close.

    The host must first drain (which reports typed loss) and stop resources.
    Returned errors remain inspectable by the caller after lifecycle teardown;
    another attempt or Local drain can never consume this attempt's failures.
    """
    current = current_remote_mutation_context()
    with _dispatch_lock:
        if current is None or any(owner == current for owner in _dispatch_owners.values()):
            raise OwnershipRejected("Original remote dispatches must drain before release")
        failures = extension_dispatch_failures()
        _dispatch_failures[:] = [(owner, error) for owner, error in _dispatch_failures if owner != current]
        _dispatch_revisions.pop(current, None)
        return failures
