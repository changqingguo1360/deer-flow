"""Cleanup fault coverage only; no installed business acceptance is inferred."""

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", ("diagnostics", "stop", "join", "client", "restore", "remove", "fleet"))
async def test_cleanup_failure_settles_all_owners_and_preserves_original_error(failing):
    from .c08_installed_cleanup import settle_owned_cleanup

    original = RuntimeError("original assertion")
    visited = []
    phases = ("diagnostics", "stop", "execution", "join", "client", "http", "restore", "remove", "user", "config", "fleet")

    def action(name):
        async def execute():
            visited.append(name)
            if name == failing:
                raise ValueError(name)

        return execute

    with pytest.raises(BaseExceptionGroup) as errors:
        await settle_owned_cleanup([(name, action(name)) for name in phases], original_error=original)
    assert visited == list(phases)
    assert errors.value.exceptions[0] is original
    assert len(errors.value.exceptions) == 2 and str(errors.value.exceptions[1]) == failing


@pytest.mark.asyncio
async def test_clean_cleanup_keeps_original_exception_for_callers_finally():
    from .c08_installed_cleanup import settle_owned_cleanup

    visited = []
    await settle_owned_cleanup([("sync", lambda: visited.append("sync"))], original_error=RuntimeError("original propagates from outer finally"))
    assert visited == ["sync"]
