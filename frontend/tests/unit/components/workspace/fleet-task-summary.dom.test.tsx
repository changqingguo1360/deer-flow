import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";

import { ThreadBackgroundTasks } from "@/components/workspace/thread-background-tasks";
import type { FleetTask } from "@/core/fleet/types";
import { enUS } from "@/core/i18n/locales/en-US";

import observed from "../../core/fleet/observed-summary.json";

const state = rs.hoisted(() => ({
  enabled: false,
  tasks: [] as FleetTask[],
  cancel: rs.fn(),
}));
rs.mock("@/core/features", () => ({
  useMcpTasksEnabled: () => ({ enabled: state.enabled }),
}));
rs.mock("@/core/i18n/hooks", () => ({ useI18n: () => ({ t: enUS }) }));
rs.mock("@/core/fleet/hooks", () => ({
  useFleetTasks: () => ({ data: state.tasks, isError: false }),
  useFleetGoalOperation: () => ({ mutate: state.cancel, isPending: false }),
}));
rs.mock("@/core/background-tasks/hooks", () => ({
  useBackgroundTasks: () => ({ data: [], isLoading: false, isError: false }),
  useBackgroundTask: () => ({ data: undefined }),
  useCancelBackgroundTask: () => ({ mutate: rs.fn(), isPending: false }),
}));
afterEach(() => {
  cleanup();
  state.cancel.mockReset();
});

describe("existing panel remote Agent cards", () => {
  it("shows actual C with MCP off and cancels its original owned goal without a false stop", async () => {
    state.enabled = false;
    state.tasks = [observed.running as FleetTask];
    const view = render(<ThreadBackgroundTasks threadId="thread-c10-http" />);
    fireEvent.click(screen.getByTestId("background-tasks-trigger"));
    expect(await screen.findByText("Running remotely")).toBeDefined();
    fireEvent.click(
      screen.getByRole("button", { name: enUS.fleetTasks.cancelGoal }),
    );
    expect(state.cancel).toHaveBeenCalledWith({
      action: "cancel",
      taskId: observed.running.task_id,
      expectedGeneration: observed.running.generation,
      idempotencyKey: expect.any(String),
    });
    state.tasks = [observed.cancel_intent as FleetTask];
    view.rerender(<ThreadBackgroundTasks threadId="thread-c10-http" />);
    expect(await screen.findByText("Stopping")).toBeDefined();
    expect(screen.queryByText("Cancelled")).toBeNull();
    state.enabled = true;
    view.rerender(<ThreadBackgroundTasks threadId="thread-c10-http" />);
    expect(screen.queryByText(enUS.backgroundTasks.empty)).toBeNull();
    // Controlled terminal-with-held UI case, based on the actual held contract.
    state.tasks = [
      {
        ...observed.running,
        state: "succeeded",
        run_status: "success",
      } as FleetTask,
    ];
    view.rerender(<ThreadBackgroundTasks threadId="thread-c10-http" />);
    expect(
      (await screen.findAllByText("Stop unconfirmed")).length,
    ).toBeGreaterThan(0);
    expect(screen.queryByText("Completed")).toBeNull();
    expect(screen.getByRole("link").getAttribute("href")).toContain(
      observed.running.current_run_id,
    );
  });
});
