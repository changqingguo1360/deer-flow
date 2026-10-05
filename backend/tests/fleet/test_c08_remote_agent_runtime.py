"""Historical producer intents migrated to required installed Linux Node execution.

The old native-Popen setup cannot supply the C08 Linux collector/publication
contract. Earlier setup failures remain in Task6 evidence; these cases now
observe a causal accepted-partial barrier in the original installed runner.
"""

import pytest

from .test_c08_stock_linux_workspace import _run_actual_installed_stock


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("full", "delta"))
async def test_stage_file_is_readable_before_next_original_step(tmp_path, mode):
    await _run_actual_installed_stock(tmp_path, mode, False, fault="partial-read")


@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("full", "delta"))
async def test_killed_original_runner_requires_recovery_without_replay(tmp_path, mode):
    await _run_actual_installed_stock(tmp_path, mode, False, fault="partial-kill")
